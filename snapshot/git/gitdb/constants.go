package gitdb

const (
	KindFSSnapshot        SnapshotKind = "fs"
	KindGitCommitSnapshot SnapshotKind = "gc"

	CleanFailureExitCode           int = 81
	CheckoutFailureExitCode            = 82
	ReleaseCheckoutFailureExitCode     = 83
	BundleUploadFailureExitCode        = 84
	ReadFileAllFailureExitCode         = 85
	ExportGitCommitFailureExitCode     = 86
	DBInitFailureExitCode              = 87
)

type AutoUploadDest int

const (
	AutoUploadNone AutoUploadDest = iota
	AutoUploadTags
	AutoUploadBundlestore
)

// SnapshotKind describes the kind of a Snapshot: is it an FSSnapshot or a GitCommitSnapshot
// kind instead of type because type is a keyword
type SnapshotKind string

var kinds = map[SnapshotKind]bool{
	KindFSSnapshot:        true,
	KindGitCommitSnapshot: true,
}
