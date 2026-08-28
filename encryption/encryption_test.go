package encryption

import (
	"bytes"
	"testing"
)

func TestSealOpenRoundTrip(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	c, err := New(key)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello world, this is chunk data")
	aad := []byte("sha256:deadbeef")

	nonce, ciphertext, tag, err := c.Seal(aad, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if len(ciphertext) != len(plaintext) {
		t.Fatalf("ciphertext length %d != plaintext length %d", len(ciphertext), len(plaintext))
	}
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext should not equal plaintext")
	}
	if len(nonce) != NonceSize || len(tag) != TagSize {
		t.Fatalf("unexpected nonce/tag size: %d/%d", len(nonce), len(tag))
	}

	got, err := c.Open(aad, nonce, ciphertext, tag)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("round trip mismatch: got %q want %q", got, plaintext)
	}
}

func TestOpenFailsOnTamperedCiphertext(t *testing.T) {
	key, _ := GenerateKey()
	c, _ := New(key)
	nonce, ciphertext, tag, _ := c.Seal(nil, []byte("secret payload"))
	ciphertext[0] ^= 0xFF
	if _, err := c.Open(nil, nonce, ciphertext, tag); err == nil {
		t.Fatal("expected error decrypting tampered ciphertext")
	}
}

func TestOpenFailsOnWrongAAD(t *testing.T) {
	key, _ := GenerateKey()
	c, _ := New(key)
	nonce, ciphertext, tag, _ := c.Seal([]byte("chunk-A"), []byte("secret payload"))
	if _, err := c.Open([]byte("chunk-B"), nonce, ciphertext, tag); err == nil {
		t.Fatal("expected error with mismatched aad")
	}
}

func TestOpenFailsWithWrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()
	c1, _ := New(key1)
	c2, _ := New(key2)
	nonce, ciphertext, tag, _ := c1.Seal(nil, []byte("secret payload"))
	if _, err := c2.Open(nil, nonce, ciphertext, tag); err == nil {
		t.Fatal("expected error decrypting with wrong key")
	}
}

func TestWrapUnwrapKeyRoundTrip(t *testing.T) {
	masterKey, _ := GenerateKey()
	dek, _ := GenerateKey()

	wrapped, err := WrapKey(masterKey, dek)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(wrapped, dek) {
		t.Fatal("wrapped key should not equal raw dek")
	}

	got, err := UnwrapKey(masterKey, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("unwrapped DEK does not match original")
	}
}

func TestUnwrapKeyFailsWithWrongMasterKey(t *testing.T) {
	masterKey, _ := GenerateKey()
	wrongKey, _ := GenerateKey()
	dek, _ := GenerateKey()
	wrapped, _ := WrapKey(masterKey, dek)
	if _, err := UnwrapKey(wrongKey, wrapped); err == nil {
		t.Fatal("expected error unwrapping with wrong master key")
	}
}

func TestNewRejectsBadKeySize(t *testing.T) {
	if _, err := New([]byte("too short")); err == nil {
		t.Fatal("expected error for short key")
	}
}

func TestDeterministicPlaintextLength(t *testing.T) {
	key, _ := GenerateKey()
	c, _ := New(key)
	for _, size := range []int{0, 1, 16, 1023, 4096, 4 * 1024 * 1024} {
		pt := bytes.Repeat([]byte{0xAB}, size)
		_, ct, _, err := c.Seal(nil, pt)
		if err != nil {
			t.Fatal(err)
		}
		if len(ct) != size {
			t.Fatalf("size %d: ciphertext length %d != plaintext length %d", size, len(ct), size)
		}
	}
}
