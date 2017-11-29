package bazel

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func (bf *bzFiler) Ingest(path string) (string, error) {
	var fileType string
	stat, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if stat.IsDir() {
		fileType = "directory"
	} else {
		fileType = "file"
	}
	output, err := bf.RunCmd([]string{fileType, "save", path})
	if err != nil {
		log.WithFields(
			log.Fields{
				"err":    err,
				"output": string(output),
				"path":   path,
			}).Errorf("Error saving directory %v", path)
		return "", err
	}
	id := string(output)
	return id, nil
}

func (bf *bzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	panic("not implemented")
}
