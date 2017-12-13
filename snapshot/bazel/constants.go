package bazel

const (
	invalidIdMsg         = "Expected ID to be of format bz-<sha256>-<sizeBytes>, was"
	invalidFileTypeMsg   = "Error getting fileType of path"
	noSuchFileOrDirMsg   = "no such file or directory"
	invalidSaveOutputMsg = "invalid output format"

	bzSnapshotIdPrefix = "bz"

	fsUtilCmd             = "fs_util"
	fsUtilCmdMaterialize  = "materialize"
	fsUtilCmdSave         = "save"
	fsUtilCmdDirectory    = "directory"
	fsUtilCmdFile         = "file"
	fsUtilCmdRoot         = "--root"
	fsUtilCmdLocalStore   = "--local-store-path"
	fsUtilCmdServerAddr   = "--server-address"
	fsUtilCmdGlobWildCard = "**"

	emptySha  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	emptySize = int64(0)
	emptyID   = bzSnapshotIdPrefix + "-" + emptySha + "-" + "0"
)
