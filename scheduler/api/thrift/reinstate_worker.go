package thrift

import (
	"fmt"

	"github.com/twitter/scoot/sched"
	"github.com/twitter/scoot/sched/scheduler"
	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
)

func ReinstateWorker(req *scoot.ReinstateWorkerReq, scheduler scheduler.Scheduler) error {
	schedReq, err := thriftReinstateReqToScoot(req)
	if err != nil {
		return nil
	}
	return scheduler.ReinstateWorker(schedReq)
}

func thriftReinstateReqToScoot(req *scoot.ReinstateWorkerReq) (result sched.ReinstateWorkerReq, err error) {
	if req == nil {
		return result, fmt.Errorf("nil ReinstateWorkerReq")
	}
	result.ID = req.GetID()
	result.Requestor = req.GetRequestor()
	return result, nil
}
