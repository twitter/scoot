package bazel

import (
	log "github.com/sirupsen/logrus"
)

func (bf *bzFiler) Ingest(path string) (string, error) {
	fileType, err := getFileType(path)
	if err != nil {
		return "", err
	}
	output, err := bf.RunCmd([]string{fileType, "save", path})
	if err != nil {
		log.WithFields(
			log.Fields{
				"err":  err,
				"path": path,
			}).Errorf("Error ingesting %s %s", fileType, path)
		return "", err
	}

	id := string(output)

	return id, nil
}

func (bf *bzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	log.Error("Not implemented")
	return "", nil
}
