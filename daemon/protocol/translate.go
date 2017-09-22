package protocol

import "github.com/twitter/scoot/runner"

// TODO(dbentley): we should go generate with command protoc daemon.proto --go_out=plugins=grpc:.

func FromRunnerStatus(status runner.RunStatus) *PollReply_Status {
	state := PollReply_Status_UNKNOWN
	switch status.State {
	case runner.UNKNOWN:
		state = PollReply_Status_UNKNOWN
	case runner.PENDING:
		state = PollReply_Status_PENDING
	case runner.PREPARING:
		state = PollReply_Status_PREPARING
	case runner.RUNNING:
		state = PollReply_Status_RUNNING
	case runner.FAILED, runner.ABORTED, runner.TIMEDOUT, runner.BADREQUEST:
		state = PollReply_Status_FAILED
	case runner.COMPLETE:
		state = PollReply_Status_COMPLETED
	}
	return &PollReply_Status{
		string(status.RunID),
		state,
		status.SnapshotID,
		int32(status.ExitCode),
		status.Error,
	}
}

func ToRunnerStatus(status *PollReply_Status) runner.RunStatus {
	state := runner.UNKNOWN
	switch status.State {
	case PollReply_Status_UNKNOWN:
		state = runner.UNKNOWN
	case PollReply_Status_PENDING:
		state = runner.PENDING
	case PollReply_Status_PREPARING:
		state = runner.PREPARING
	case PollReply_Status_RUNNING:
		state = runner.RUNNING
	case PollReply_Status_FAILED:
		state = runner.FAILED
	case PollReply_Status_COMPLETED:
		state = runner.COMPLETE
	}
	return runner.RunStatus{
		RunID:      runner.RunID(status.RunId),
		State:      state,
		SnapshotID: status.SnapshotId,
		ExitCode:   int(status.ExitCode),
		Error:      status.Error,
	}
}
