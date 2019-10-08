package thrift

import (
	"fmt"

	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/server"
)

func ReinstateWorker(req *scoot.ReinstateWorkerReq, scheduler server.Scheduler) error {
	schedReq, err := thriftReinstateReqToScoot(req)
	if err != nil {
		return nil
	}
	return scheduler.ReinstateWorker(schedReq)
}

func thriftReinstateReqToScoot(req *scoot.ReinstateWorkerReq) (result domain.ReinstateWorkerReq, err error) {
	if req == nil {
		return result, fmt.Errorf("nil ReinstateWorkerReq")
	}
	result.ID = req.GetID()
	result.Requestor = req.GetRequestor()
	return result, nil
}
