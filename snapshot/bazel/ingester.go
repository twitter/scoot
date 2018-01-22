package bazel

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

func (bf *BzFiler) Ingest(path string) (string, error) {
	id, err := bf.runner.save(path)
	if err != nil {
		log.WithFields(
			log.Fields{
				"err":  err,
				"path": path,
			}).Errorf("Error ingesting %s", path)
		return "", err
	}

	return id, nil
}

func (bf *BzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	errMsg := "Not implemented"
	log.Error(errMsg)
	return "", fmt.Errorf(errMsg)
}
