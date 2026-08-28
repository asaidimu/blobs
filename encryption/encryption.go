// Package encryption provides optional, per-namespace encryption-at-rest
// for the blobstore.
//
// # Design
//
// Encryption is applied at the chunk level, inside package volume, using
// AES-256-GCM — but with the authentication tag stored *detached* from
// the ciphertext (in the index's ChunkEntry record) rather than appended
// to it. That single decision is what makes encryption transparent to
// the rest of the store:
//
//   - Ciphertext is always exactly the same length as the plaintext it
//     replaces. Nothing about the on-disk page/segment layout, page-count
//     math, or CRC placement changes.
//   - Segment compaction (volume.Engine.RewriteSegment) copies page byte
//     spans verbatim without parsing payload contents — it neither needs
//     nor gets access to any key, and needed zero changes to support
//     encrypted namespaces.
//   - Streaming/seekable reads (store.NamespaceHandle.GetSeekable, used
//     by package streaming for HTTP Range requests such as video
//     scrubbing) already operate at chunk granularity: each chunk is
//     read, and now decrypted, independently. A seek to any byte offset
//     still never reads a chunk it doesn't need — decryption adds no
//     buffering requirement beyond the single chunk already being read.
//
// # Key hierarchy
//
// Each namespace that opts into encryption gets its own randomly
// generated 256-bit data-encryption key (DEK), created once by
// Store.CreateNamespace and never changed. The DEK itself is never
// stored in the clear: it is wrapped (encrypted) under a caller-supplied
// master key before being persisted in the namespace's index record, via
// WrapKey/UnwrapKey. The store never generates, stores, or has any
// opinion about where the master key comes from — that's the KeyProvider
// the caller supplies (an env var, a file, a KMS, a secrets manager —
// anything). This keeps master-key custody entirely outside this
// library, which is the appropriate boundary for a storage engine.
//
// # What this does not cover
//
//   - Staged, not-yet-committed upload data (package staging) is written
//     to its own temporary files independent of this package and is not
//     encrypted by it.
//   - There is no in-place migration path: enabling encryption only
//     affects chunks written after it is turned on for a namespace.
//     Encrypting an existing, populated namespace requires a full
//     re-write of its data (decrypt-or-read the old chunks, write them
//     back through an Engine configured with a Cipher).
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const (
	// KeySize is the length in bytes of an AES-256 key — both a
	// namespace DEK and a master key must be exactly this long.
	KeySize = 32

	// NonceSize is the length in bytes of a GCM nonce.
	NonceSize = 12

	// TagSize is the length in bytes of a GCM authentication tag.
	TagSize = 16
)

// Cipher encrypts and decrypts chunk payloads for a single namespace. A
// Cipher is constructed from that namespace's (unwrapped) DEK and holds
// no other state — it is safe for concurrent use, since crypto/cipher's
// AEAD implementations are.
type Cipher struct {
	aead cipher.AEAD
}

// New constructs a Cipher from a raw 32-byte AES-256 key. The key is
// typically a namespace's DEK, already unwrapped via UnwrapKey.
func New(key []byte) (*Cipher, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("encryption: key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("encryption: new AES cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: new GCM: %w", err)
	}
	if aead.NonceSize() != NonceSize {
		// Defensive: crypto/cipher.NewGCM always returns a 12-byte-nonce
		// AEAD for the standard construction used here, but this guards
		// against that ever silently changing out from under the fixed
		// NonceSize contract the rest of this package (and the on-disk
		// ChunkEntry.Nonce field) relies on.
		return nil, fmt.Errorf("encryption: unexpected GCM nonce size %d, want %d", aead.NonceSize(), NonceSize)
	}
	if aead.Overhead() != TagSize {
		return nil, fmt.Errorf("encryption: unexpected GCM tag size %d, want %d", aead.Overhead(), TagSize)
	}
	return &Cipher{aead: aead}, nil
}

// GenerateKey returns a new random 32-byte AES-256 key, suitable for use
// as either a namespace DEK or a master key.
func GenerateKey() ([]byte, error) {
	key := make([]byte, KeySize)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("encryption: generate key: %w", err)
	}
	return key, nil
}

// Seal encrypts plaintext, returning a fresh random nonce, the
// ciphertext (always exactly len(plaintext) bytes), and a detached
// authentication tag. The caller must persist all three — nonce and tag
// are required to decrypt, and are not recoverable from the ciphertext
// alone.
//
// aad (additional authenticated data) is bound to the ciphertext but not
// encrypted: an Open call must supply the identical aad or verification
// fails. Callers should pass something that uniquely and immutably
// identifies what's being encrypted — e.g. the chunk's own content-hash
// ID — so a ciphertext can never be silently relabeled as belonging to a
// different chunk, even by someone able to rewrite raw on-disk bytes.
// Pass nil if there is no such context to bind.
func (c *Cipher) Seal(aad, plaintext []byte) (nonce, ciphertext, tag []byte, err error) {
	nonce = make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, nil, fmt.Errorf("encryption: generate nonce: %w", err)
	}
	sealed := c.aead.Seal(nil, nonce, plaintext, aad)
	// crypto/cipher's GCM always appends the tag as the final Overhead()
	// (== TagSize, checked in New) bytes of Seal's output — split it off
	// so ciphertext stays exactly len(plaintext) bytes.
	split := len(sealed) - TagSize
	return nonce, sealed[:split], sealed[split:], nil
}

// Open decrypts ciphertext using nonce and the detached tag, verifying
// aad exactly as it was passed to the corresponding Seal call. It never
// returns partial or unverified plaintext: if the tag doesn't match —
// because the ciphertext, nonce, tag, or aad was altered, corrupted, or
// mismatched — it returns an error and a nil slice.
func (c *Cipher) Open(aad, nonce, ciphertext, tag []byte) ([]byte, error) {
	if len(nonce) != NonceSize {
		return nil, fmt.Errorf("encryption: nonce must be %d bytes, got %d", NonceSize, len(nonce))
	}
	if len(tag) != TagSize {
		return nil, fmt.Errorf("encryption: tag must be %d bytes, got %d", TagSize, len(tag))
	}
	sealed := make([]byte, 0, len(ciphertext)+len(tag))
	sealed = append(sealed, ciphertext...)
	sealed = append(sealed, tag...)
	plaintext, err := c.aead.Open(nil, nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("encryption: authentication failed (corrupted or tampered data): %w", err)
	}
	return plaintext, nil
}

// ── Envelope key wrapping ────────────────────────────────────────────────────

// KeyProvider supplies the master key(s) used to wrap and unwrap
// per-namespace DEKs. The store never generates, stores, or persists a
// master key itself — acquiring and protecting it (env var, file, KMS,
// secrets manager, etc.) is entirely the caller's responsibility.
//
// Implementations must be safe for concurrent use.
type KeyProvider interface {
	// CurrentKey returns the master key to use when wrapping a *new*
	// DEK, along with an opaque, non-empty version identifier for it.
	// The version is stored alongside the wrapped DEK so a later
	// UnwrapKey call knows which key to ask for — this is what makes
	// master-key rotation possible without touching already-wrapped
	// DEKs: old namespaces keep resolving their original version, new
	// namespaces get the new one.
	CurrentKey() (key []byte, version string, err error)

	// Key returns the master key for a specific version, for unwrapping
	// a DEK that may have been wrapped under an older master key than
	// the current one.
	Key(version string) (key []byte, err error)
}

// WrapKey encrypts dek (a namespace's DEK) under masterKey, producing a
// single self-contained blob suitable for persisting in the namespace's
// index record. masterKey must be 32 bytes.
func WrapKey(masterKey, dek []byte) ([]byte, error) {
	c, err := New(masterKey)
	if err != nil {
		return nil, fmt.Errorf("encryption: wrap key: %w", err)
	}
	nonce, ciphertext, tag, err := c.Seal(nil, dek)
	if err != nil {
		return nil, fmt.Errorf("encryption: wrap key: %w", err)
	}
	wrapped := make([]byte, 0, NonceSize+TagSize+len(ciphertext))
	wrapped = append(wrapped, nonce...)
	wrapped = append(wrapped, tag...)
	wrapped = append(wrapped, ciphertext...)
	return wrapped, nil
}

// UnwrapKey reverses WrapKey, recovering the original DEK. Returns an
// error if masterKey is not the key wrap was called with, or if wrapped
// has been truncated or corrupted.
func UnwrapKey(masterKey, wrapped []byte) ([]byte, error) {
	if len(wrapped) < NonceSize+TagSize {
		return nil, fmt.Errorf("encryption: unwrap key: wrapped key too short (%d bytes)", len(wrapped))
	}
	c, err := New(masterKey)
	if err != nil {
		return nil, fmt.Errorf("encryption: unwrap key: %w", err)
	}
	nonce := wrapped[:NonceSize]
	tag := wrapped[NonceSize : NonceSize+TagSize]
	ciphertext := wrapped[NonceSize+TagSize:]
	dek, err := c.Open(nil, nonce, ciphertext, tag)
	if err != nil {
		return nil, fmt.Errorf("encryption: unwrap key: %w", err)
	}
	return dek, nil
}
