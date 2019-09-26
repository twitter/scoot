package api

import (
	"fmt"

	"github.com/twitter/scoot/scheduler/sched"
	"github.com/twitter/scoot/scheduler/sched/scheduler"
	"github.com/twitter/scoot/scheduler/scootapi/gen-go/scoot"
)

func OfflineWorker(req *scoot.OfflineWorkerReq, scheduler scheduler.Scheduler) error {
	schedReq, err := thriftOfflineReqToScoot(req)
	if err != nil {
		return nil
	}
	return scheduler.OfflineWorker(schedReq)
}

func thriftOfflineReqToScoot(req *scoot.OfflineWorkerReq) (result sched.OfflineWorkerReq, err error) {
	if req == nil {
		return result, fmt.Errorf("nil OfflineWorkerRequest")
	}
	result.ID = req.GetID()
	result.Requestor = req.GetRequestor()
	return result, nil
}
