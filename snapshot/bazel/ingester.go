package bazel

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func (bf *bzFiler) Ingest(path string) (string, error) {
	fileType, err := bf.getFileType(path)
	if err != nil {
		return "", err
	}
	output, err := bf.RunCmd([]string{fileType, "save", path})
	if err != nil {
		log.WithFields(
			log.Fields{
				"err":    err,
				"output": string(output),
				"path":   path,
			}).Errorf("Error ingesting %s %s", fileType, path)
		return "", err
	}

	id := string(output)
	return id, nil
}

func (bf *bzFiler) IngestMap(srcToDest map[string]string) (string, error) {
	panic("not implemented")
}

// Used as arg for fs_util binary
func (bf *bzFiler) getFileType(path string) (string, error) {
	var fileType string
	stat, err := os.Stat(path)
	if err != nil {
		log.Errorf("%s %s: %v", invalidFileTypeMsg, path, err)
		return fileType, err
	}

	switch mode := stat.Mode(); {
	case mode.IsDir():
		fileType = "directory"
	case mode.IsRegular():
		fileType = "file"
	}
	return fileType, err
}
