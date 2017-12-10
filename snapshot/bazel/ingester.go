package bazel

import (
	log "github.com/sirupsen/logrus"
)

func (bf *bzFiler) Ingest(path string) (string, error) {
	output, err := bf.command.save(path)
	if err != nil {
		log.WithFields(
			log.Fields{
				"err":  err,
				"path": path,
			}).Errorf("Error ingesting %s", path)
		return "", err
	}

	return string(output), nil
}

func (bf *bzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	log.Error("Not implemented")
	return "", nil
}
