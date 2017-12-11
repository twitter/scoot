package bazel

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/twitter/scoot/bazel"

	log "github.com/sirupsen/logrus"
)

// Used as arg for fs_util binary
func getFileType(path string) (string, error) {
	var fileType string
	stat, err := os.Stat(path)
	if err != nil {
		log.Errorf("%s %s: %v", invalidFileTypeMsg, path, err)
		return fileType, err
	}

	switch mode := stat.Mode(); {
	case mode.IsDir():
		fileType = fsUtilCmdDirectory
	case mode.IsRegular():
		fileType = fsUtilCmdFile
	default:
		return "", fmt.Errorf("%s %s", invalidFileTypeMsg, path)
	}
	return fileType, err
}

// Checks that ID is well formed
func validateID(id string) error {
	sha, err := getSha(id)
	if err != nil {
		return err
	}
	size, err := getSize(id)
	if err != nil {
		return err
	}
	if !bazel.IsValidDigest(sha, size) {
		return fmt.Errorf("Error: Invalid digest. SHA: %s, size: %d", sha, size)
	}
	return nil
}

// Checks that bytes of ID is well formed
func validateIDBytes(output []byte) error {
	return validateID(string(output))
}

func getSha(id string) (string, error) {
	s, err := splitId(id)
	if err != nil {
		return "", err
	}
	return s[1], nil
}

func getSize(id string) (int64, error) {
	s, err := splitId(id)
	if err != nil {
		return 0, err
	}
	size, err := strconv.ParseInt(s[2], 10, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func splitId(id string) ([]string, error) {
	s := strings.Split(id, "-")
	if len(s) < 3 || s[0] != bzSnapshotIdPrefix {
		return nil, fmt.Errorf("%s %s", invalidIdMsg, id)
	}
	return s, nil
}

func generateId(sha string, size int64) string {
	return fmt.Sprintf("%s-%s-%d", bzSnapshotIdPrefix, sha, size)
}
