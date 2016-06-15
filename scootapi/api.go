package scootapi

// We should use go generate to run thrift --gen go:package_prefix=github.com/scootdev/scoot/scootapi/gen-go/ scoot.thrift

import (
	"github.com/scootdev/scoot/scootapi/gen-go/scoot"
)

type Handler struct{}

func NewHandler() scoot.Proc {
	return &Handler{}
}

func (h *Handler) RunJob(def *scoot.JobDefinition) (*scoot.Job, error) {
	r := scoot.NewInvalidRequest()
	msg := "Scoot is working by saying it won't work"
	r.Message = &msg
	return nil, r
}
