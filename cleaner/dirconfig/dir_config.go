package dirconfig

// A DirConfig defines the manner in which a particular directory should be cleaned
type DirConfig interface {
	CleanDir() error
	GetDir() string
}
