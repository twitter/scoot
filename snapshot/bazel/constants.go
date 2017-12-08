package bazel

const (
	invalidIdMsg       = "Expected ID to be of format bz-<sha256>-<sizeBytes>, was"
	invalidFileTypeMsg = "Error getting fileType of path"
	noSuchFileOrDirMsg = "no such file or directory"

	bzSnapshotIdPrefix = "bz"

	fsUtilCmdMaterialize = "materialize"
	fsUtilCmdDirectory   = "directory"
	fsUtilCmdFile        = "file"
)
