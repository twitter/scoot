package thrift

import (
	"fmt"

	"github.com/twitter/scoot/scheduler/api/thrift/gen-go/scoot"
	"github.com/twitter/scoot/scheduler/domain"
	"github.com/twitter/scoot/scheduler/server"
)

func OfflineWorker(req *scoot.OfflineWorkerReq, scheduler server.Scheduler) error {
	schedReq, err := thriftOfflineReqToScoot(req)
	if err != nil {
		return nil
	}
	return scheduler.OfflineWorker(schedReq)
}

func thriftOfflineReqToScoot(req *scoot.OfflineWorkerReq) (result domain.OfflineWorkerReq, err error) {
	if req == nil {
		return result, fmt.Errorf("nil OfflineWorkerRequest")
	}
	result.ID = req.GetID()
	result.Requestor = req.GetRequestor()
	return result, nil
}
