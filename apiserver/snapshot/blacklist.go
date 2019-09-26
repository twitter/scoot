package snapshot

import (
	"errors"
)

// A Snapshots that wraps another Snapshots and blacklists certain files

// Note: can only blacklist files at the top-level of a snapshot.

type blacklistSnapshots struct {
	del       Snapshots
	blacklist map[string]bool // paths in this blacklist are blacklisted
}

// Creates a new Blacklisting Snapshots that delegates to delegate but blacklists keys in blacklist
func NewBlacklistSnapshots(delegate Snapshots, blacklist map[string]bool) Snapshots {
	return &blacklistSnapshots{delegate, blacklist}
}

// Creates a new Blacklisting Snapshot that delegates to delegate but blacklists keys in blacklist
func NewBlacklistSnapshot(delegate Snapshot, blacklist map[string]bool) Snapshot {
	return &blacklistSnapshot{delegate, blacklist}
}

func (s *blacklistSnapshots) Get(id string) (Snapshot, error) {
	snap, err := s.del.Get(id)
	if err != nil {
		return nil, err
	}
	return &blacklistSnapshot{snap, s.blacklist}, nil
}

type blacklistSnapshot struct {
	del       Snapshot
	blacklist map[string]bool
}

func (s *blacklistSnapshot) Id() string {
	return s.del.Id()
}

func (s *blacklistSnapshot) Lstat(name string) (FileInfo, error) {
	if _, ok := s.blacklist[name]; ok {
		return nil, &pathError{errors.New("No such file")}
	}
	return s.del.Lstat(name)
}

func (s *blacklistSnapshot) Stat(name string) (FileInfo, error) {
	if _, ok := s.blacklist[name]; ok {
		return nil, &pathError{errors.New("No such file")}
	}
	return s.del.Stat(name)
}

func (s *blacklistSnapshot) Readdirents(name string) ([]Dirent, error) {
	if _, ok := s.blacklist[name]; ok {
		return nil, &pathError{errors.New("No such directory")}
	}

	if name != "" {
		// We only blacklist at the top-level. But this is a readdir
		// for a not top-level directory, so just return the delegate's response.
		return s.del.Readdirents(name)
	}

	dirents, err := s.del.Readdirents(name)
	if err != nil {
		return nil, err
	}

	r := make([]Dirent, len(dirents))
	numResults := 0

	for _, dirent := range dirents {
		if _, ok := s.blacklist[dirent.Name]; ok {
			continue
		}
		r[numResults] = dirent
		numResults++
	}
	return r[0:numResults], nil
}

func (s *blacklistSnapshot) Readlink(name string) (string, error) {
	if _, ok := s.blacklist[name]; ok {
		return "", &pathError{errors.New("No such directory")}
	}

	return s.del.Readlink(name)
}

func (s *blacklistSnapshot) Open(name string) (File, error) {
	if _, ok := s.blacklist[name]; ok {
		return nil, &pathError{errors.New("No such directory")}
	}

	return s.del.Open(name)
}
