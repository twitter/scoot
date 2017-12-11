package bazel

const (
	invalidIdMsg         = "Expected ID to be of format bz-<sha256>-<sizeBytes>, was"
	invalidFileTypeMsg   = "Error getting fileType of path"
	noSuchFileOrDirMsg   = "no such file or directory"
	invalidSaveOutputMsg = "invalid output format"

	bzSnapshotIdPrefix = "bz"

	fsUtilCmd            = "fs_util"
	fsUtilCmdMaterialize = "materialize"
	fsUtilCmdSave        = "save"
	fsUtilCmdDirectory   = "directory"
	fsUtilCmdFile        = "file"
)
