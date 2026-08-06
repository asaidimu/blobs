//go:build darwin

package staging

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// preallocate reserves size bytes of real disk space for f using the
// F_PREALLOCATE fcntl (APFS/HFS+ only; other filesystems, such as network
// mounts, don't support it and fall back to a plain truncate below).
//
// F_PREALLOCATE first requests a contiguous extent. On volumes that are too
// fragmented to satisfy that, the kernel returns ENOSPC even though enough
// free space exists in total, so we retry once without the contiguity
// requirement before giving up. F_PREALLOCATE only reserves blocks; it does
// not change the file's logical size, so we still need Truncate afterward
// to set the EOF that the rest of the package (WriteAt, Stat-based size
// checks) relies on.
func preallocate(f *os.File, size int64) error {
	store := &unix.Fstore_t{
		Flags:   unix.F_ALLOCATECONTIG | unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}

	err := fcntlFstore(f.Fd(), unix.F_PREALLOCATE, store)
	if err != nil {
		// Contiguous allocation failed (commonly ENOSPC on a fragmented
		// volume even though total free space is sufficient). Retry
		// without the contiguity requirement before giving up on it.
		store.Flags = unix.F_ALLOCATEALL
		if err2 := fcntlFstore(f.Fd(), unix.F_PREALLOCATE, store); err2 != nil {
			if err == unix.ENOSPC || err2 == unix.ENOSPC {
				// Genuinely out of space: surface this now so the
				// caller (staging.Begin) fails fast instead of
				// discovering it mid-upload.
				return err2
			}
			// F_PREALLOCATE itself isn't supported on this filesystem
			// (e.g. SMB/NFS mounts, some non-APFS/HFS+ volumes report
			// EINVAL/ENOTSUP here). Fall back to a plain truncate: this
			// yields a sparse file with no real-space guarantee, matching
			// the behavior of preallocate_other.go on platforms that
			// have no native preallocation call at all.
			return f.Truncate(size)
		}
	}

	// Reservation succeeded; set the logical size to match.
	return f.Truncate(size)
}

// fcntlFstore issues fcntl(fd, cmd, &store). golang.org/x/sys/unix has no
// typed helper for struct-based fcntl commands, so this goes through the
// raw syscall the same way the package's int-based unix.Fcntl does.
func fcntlFstore(fd uintptr, cmd int, store *unix.Fstore_t) error {
	_, _, errno := unix.Syscall(unix.SYS_FCNTL, fd, uintptr(cmd), uintptr(unsafe.Pointer(store)))
	if errno != 0 {
		return errno
	}
	return nil
}
