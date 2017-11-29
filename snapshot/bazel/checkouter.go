package bazel

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/snapshot"
)

const defaultDir = "/tmp/scoot-bazel-snapshot-dir"

// We expect bazel IDs to be of format bz-<sha256>-<sizeBytes>
func (bf *bzFiler) Checkout(id string) (snapshot.Checkout, error) {
	return bf.CheckoutAt(id, defaultDir)
}

func (bf *bzFiler) CheckoutAt(id string, dir string) (snapshot.Checkout, error) {
	s := strings.Split(id, "-")
	sha, sizeString := s[1], s[2]
	size, err := strconv.ParseInt(sizeString, 10, 64)
	if err != nil {
		return nil, err
	}
	if !bazel.IsValidDigest(sha, size) {
		return nil, fmt.Errorf("Error: Invalid digest. SHA: %v, size: %d", sha, size)
	}
	output, err := bf.RunCmd([]string{"directory", "materialize", sha, dir})
	if err != nil {
		log.WithFields(
			log.Fields{
				"error":  err,
				"output": string(output),
				"sha":    sha,
				"dir":    dir,
			}).Error("Failed to materialize directory")
		log.Error(string(output))
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
