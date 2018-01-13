package bazel

import (
	"path"

	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
)

func (bf *BzFiler) Checkout(id string) (snapshot.Checkout, error) {
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		return nil, err
	}
	return bf.CheckoutAt(id, path.Join(tempDir.Dir, snapshotDirName))
}

func (bf *BzFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	err := bazel.ValidateID(id)
	if err != nil {
		return nil, err
	}
	sha, err := bazel.GetSha(id)
	if err != nil {
		return nil, err
	}
	size, err := bazel.GetSize(id)
	if err != nil {
		return nil, err
	}

	err = bf.command.materialize(sha, dir)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error": err,
				"sha":   sha,
				"dir":   dir,
			}).Errorf("Failed to materialize %s", id)
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
	return co, nil
}
