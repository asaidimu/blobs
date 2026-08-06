//go:build !linux && !darwin

package staging

import "os"

func preallocate(f *os.File, size int64) error {
	return f.Truncate(size)
}
