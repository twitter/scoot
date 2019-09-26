package snapshot

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// Adapted from golang's syscall/syscall_darwin.go:ParseDirent to include more info
// about the dirent.

func parseDirent(buf []byte, dirents []Dirent) []Dirent {
	for len(buf) > 0 {
		dirent := (*unix.Dirent)(unsafe.Pointer(&buf[0]))
		buf = buf[dirent.Reclen:]
		if dirent.Ino == 0 { // File absent in directory.
			continue
		}
		bytes := (*[10000]byte)(unsafe.Pointer(&dirent.Name[0]))
		var name = string(bytes[0:clen(bytes[:])])
		if name == "." || name == ".." { // Useless names
			continue
		}
		var out Dirent
		out.Name = name
		switch FileType(dirent.Type) {
		case FT_Directory:
			out.Type = FT_Directory
		case FT_File:
			out.Type = FT_File
		case FT_Symlink:
			out.Type = FT_Symlink
		default:
			out.Type = FT_Unknown
		}
		dirents = append(dirents, out)
	}
	return dirents
}

func clen(n []byte) int {
	for i := 0; i < len(n); i++ {
		if n[i] == 0 {
			return i
		}
	}
	return len(n)
}
