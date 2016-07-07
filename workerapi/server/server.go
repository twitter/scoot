package server

import (
	"fmt"
	"log"
	"strconv"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/luci/go-render/render"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

func Serve(handler worker.Worker, addr string, transportFactory thrift.TTransportFactory, protocolFactory thrift.TProtocolFactory) error {
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		return err
	}
	processor := worker.NewWorkerProcessor(handler)
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	fmt.Println("About to serve")

	return server.Serve()
}

type Handler struct {
	runs map[string]*worker.RunStatus
}

func NewHandler() worker.Worker {
	return &Handler{make(map[string]*worker.RunStatus)}
}

func (h *Handler) QueryWorker() (*worker.WorkerStatus, error) {
	wstatus := worker.NewWorkerStatus()
	wstatus.Running = []string{}
	for runId, status := range h.runs {
		if status.Status == worker.Status_RUNNING {
			wstatus.Running = append(wstatus.Running, runId)
		}
	}
	return wstatus, nil
}

func makeStatus(status worker.Status, exitCode int32, info string) *worker.RunStatus {
	rs := worker.NewRunStatus()
	rs.Status = status
	rs.Info = &info
	rs.ExitCode = &exitCode
	return rs
}

//TODO: integrate runner lib and remove all this temp test code.
func (h *Handler) Run(cmd *worker.RunCommand) (*worker.RunStatus, error) {
	numRuns := len(h.runs)
	runId := strconv.Itoa(numRuns)
	prevRunId := strconv.Itoa(numRuns - 1)

	log.Println("Running", render.Render(cmd), runId)
	if numRuns > 0 && h.runs[prevRunId].Status == worker.Status_RUNNING {
		return makeStatus(worker.Status_UNKNOWN, -1, "A cmd is already running"), nil
	}

	devNull := "/dev/null"
	status := makeStatus(worker.Status_COMPLETED, -1, "Worker is working by saying it won't work")
	status.OutUri = &devNull
	status.ErrUri = &devNull
	status.RunId = &runId
	if len(cmd.Argv) == 1 && cmd.Argv[0] == "sleep" {
		exitCode := int32(0)
		status.Status = worker.Status_RUNNING
		status.ExitCode = &exitCode
	}

	h.runs[runId] = status
	return status, nil
}

func (h *Handler) Query(runId string) (*worker.RunStatus, error) {
	if status, ok := h.runs[runId]; ok {
		return status, nil
	}
	info := "RunId not found"
	status := worker.NewRunStatus()
	status.Status = worker.Status_UNKNOWN
	status.Info = &info
	return status, nil
}

func (h *Handler) Abort(runId string) (*worker.RunStatus, error) {
	if status, ok := h.runs[runId]; ok && status.Status != worker.Status_COMPLETED && status.Status != worker.Status_ABORTED {
		info := "Aborted"
		status.Status = worker.Status_ABORTED
		status.Info = &info
		return status, nil
	}
	info := "RunId not found or not abortable"
	status := worker.NewRunStatus()
	status.Status = worker.Status_UNKNOWN
	status.Info = &info
	return status, nil
}
