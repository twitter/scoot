package snapshots

import (
	"syscall"
)

// Adapated from golang's os/dir_unix.go:Readdirnames

// Readdirnames is complex because it tries to be restartable.
// We have made our code simpler by fetching all dirents at once.
// This makes our code simpler, but at the cost of having to do more.
// We may want to revisit it to be more stateful in a way that makes our
// clients' jobs easier and perform better.

func readDirents(fd int) ([]Dirent, error) {
	// TODO(dbentley): should we allocate with a capacity?
	// golang's OS defaults to 100
	r := make([]Dirent, 0)
	buf := make([]byte, 4096) // 4096 = blocksize
	for {
		// Fill the buffer
		n, err := syscall.ReadDirent(fd, buf)
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return r, nil
		}

		// Drain the buffer
		r = parseDirent(buf[:n], r)
		// TODO(dbentley): we could exit here if we notice that we have
		// parsed no dirents, which could maybe save us an extra
		// ReadDirent syscall.
	}
}
