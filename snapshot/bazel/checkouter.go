package bazel

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
)

func (bf *bzFiler) Checkout(id string) (snapshot.Checkout, error) {
	tempDir, err := temp.TempDirDefault()
	if err != nil {
		return nil, err
	}
	return bf.CheckoutAt(id, tempDir.Dir)
}

func (bf *bzFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	// We expect bazel IDs to be of format bz-<sha256>-<sizeBytes>
	s := strings.Split(id, "-")
	if len(s) < 3 {
		return nil, fmt.Errorf("%s %s", invalidIdMsg, id)
	}

	sha, sizeString := s[1], s[2]
	size, err := strconv.ParseInt(sizeString, 10, 64)
	if err != nil {
		return nil, err
	}

	if !bazel.IsValidDigest(sha, size) {
		return nil, fmt.Errorf("Error: Invalid digest. SHA: %s, size: %d", sha, size)
	}

	_, err = bf.command.materialize(sha, dir)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error": err,
				"sha":   sha,
				"dir":   dir,
			}).Error("Failed to materialize %s", id)
		return nil, err
	}

	co := &bzCheckout{
		dir,
		remoteexecution.Digest{
			Hash:      sha,
			SizeBytes: size,
		},
	}
	return co, nil
}
