//go:build linux

package staging

import (
	"os"

	"golang.org/x/sys/unix"
)

func preallocate(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}
