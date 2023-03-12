package helpers

import (
	log "github.com/sirupsen/logrus"

	"github.com/wisechengyi/scoot/scheduler/api/thrift/gen-go/scoot"
)

func CopyStringToPointer(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func CopyPointerToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func CopyPointerToInt32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}

func LogRunStatus(status *scoot.JobStatus) {
	log.Info("Task Data:")
	for task, runStatus := range status.GetTaskData() {
		if runStatus == nil {
			log.Infof("%s: ExitCode: <nil>, Error: <nil>", task)
			continue
		}
		log.Infof("%s: ExitCode: %v, Error: %v", task, runStatus.GetExitCode(), runStatus.GetError())
	}
}
