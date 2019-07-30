package bazel

import (
	"github.com/twitter/scoot/bazel"
)

const (
	invalidFileTypeMsg   = "Error getting fileType of path"
	noSuchFileOrDirMsg   = "no such file or directory"
	invalidSaveOutputMsg = "invalid output format"

	snapshotDirName = "snapshot"

	fsUtilCmd             = "fs_util"
	fsUtilCmdMaterialize  = "materialize"
	fsUtilCmdSave         = "save"
	fsUtilCmdDirectory    = "directory"
	fsUtilCmdFile         = "file"
	fsUtilCmdRoot         = "--root"
	fsUtilCmdLocalStore   = "--local-store-path"
	fsUtilCmdServerAddr   = "--server-address"
	fsUtilCmdGlobWildCard = "**"

	emptyID = bazel.SnapshotIDPrefix + "-" + bazel.EmptySha + "-0"

	// limit amount of CAS addresses resolved in BzFiler calls to fs_util to this
	// value, in order to limit number of connections
	maxResolveToFSUtil = 5
)
