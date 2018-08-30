package bazel

import (
	"fmt"
	"os"
	"path"

	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/snapshot"
)

// Type that indicates a Checkout operation failed because one or more input elements
// did not exist. This does not currently hold specific data, but could in the future
// include specific Digest information. This is limited by output from the underlying
// Filer -> BzTree implementation that uses the fs_util tool (materialize command).
type CheckoutNotExistError struct {
	Err string
}

// Implements the Error interface
func (c *CheckoutNotExistError) Error() string {
	if c == nil {
		return ""
	}
	return c.Err
}

func (bf *BzFiler) Checkout(id string) (snapshot.Checkout, error) {
	tmp, err := bf.tmp.TempDir("checkout")
	if err != nil {
		return nil, err
	}
	return bf.CheckoutAt(id, path.Join(tmp.Dir, snapshotDirName))
}

func (bf *BzFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	err := bazel.ValidateID(id)
	if err != nil {
		return nil, err
	}
	sha, size, err := bazel.GetShaAndSize(id)
	if err != nil {
		return nil, err
	}

	co := &bzCheckout{
		dir,
		bf.keepCheckouts,
		remoteexecution.Digest{
			Hash:      sha,
			SizeBytes: size,
		},
	}

	err = bf.tree.materialize(sha, size, dir)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error": err,
				"sha":   sha,
				"dir":   dir,
			}).Errorf("Failed to Materialize %s", id)
		return nil, err
	}
	parentDir, _ := path.Split(co.Path())
	err = bf.setUpJDKSymlink(parentDir)
	if err != nil {
		return nil, err
	}

	return co, nil
}

func (bf *BzFiler) setUpJDKSymlink(dir string) error {
	l := <-bf.JDKSymlinkCh
	sl, ok := l.(string)
	if !ok {
		// nil is sent on bf.JDKSymlinkCh when no JDK_SYMLINK property is defined
		return nil
	}
	javaHome, ok := os.LookupEnv("JAVA_HOME")
	if !ok {
		return fmt.Errorf("Failed setting up platform property JDK_SYMLINK: $JAVA_HOME not set")
	}
	return os.Symlink(javaHome, path.Join(dir, sl))
}
