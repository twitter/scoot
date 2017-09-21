package workerapi

import (
	"reflect"
	"testing"
	"time"

	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/workerapi/gen-go/worker"
)

var someCmd = []string{"", "x"}
var someEnv = map[string]string{"a": "1", "b": "2"}
var zero = int32(0)
var nonzero = int32(12345)
var emptystr = ""
var nonemptystr = "abcdef"
var deadbeefID = "snap-id-deadbeef"

var cmdFromThrift = func(x interface{}) interface{} { return ThriftRunCommandToDomain(x.(*worker.RunCommand)) }
var cmdToThrift = func(x interface{}) interface{} { return DomainRunCommandToThrift(x.(*runner.Command)) }
var rsFromThrift = func(x interface{}) interface{} { return ThriftRunStatusToDomain(x.(*worker.RunStatus)) }
var rsToThrift = func(x interface{}) interface{} { return DomainRunStatusToThrift(x.(runner.RunStatus)) }
var wsFromThrift = func(x interface{}) interface{} { return ThriftWorkerStatusToDomain(x.(*worker.WorkerStatus)) }
var wsToThrift = func(x interface{}) interface{} { return DomainWorkerStatusToThrift(x.(WorkerStatus)) }

var tests = []struct {
	id             int
	thriftToDomain func(interface{}) interface{}
	domainToThrift func(interface{}) interface{}
	thrift         interface{}
	domain         interface{}
}{
	//Cmd
	{
		0,
		cmdFromThrift,
		nil,
		&worker.RunCommand{
			Argv:       []string{},
			Env:        nil,
			SnapshotId: nil,
			TimeoutMs:  nil,
			JobId:      &deadbeefID,
			TaskId:     &deadbeefID,
			Tag:        &deadbeefID},
		&runner.Command{
			Argv:    []string{},
			EnvVars: map[string]string{},
			Timeout: time.Duration(zero),
			LogTags: tags.LogTags{
				JobID:  deadbeefID,
				TaskID: deadbeefID,
				Tag:    deadbeefID,
			},
		},
	},
	{
		1,
		cmdFromThrift,
		cmdToThrift,
		&worker.RunCommand{
			Argv:       []string{},
			Env:        map[string]string{},
			SnapshotId: &emptystr,
			TimeoutMs:  &zero,
			JobId:      &nonemptystr,
			TaskId:     &emptystr,
			Tag:        &emptystr},
		&runner.Command{
			Argv:    []string{},
			EnvVars: map[string]string{},
			Timeout: time.Duration(zero),
			LogTags: tags.LogTags{
				JobID:  nonemptystr,
				TaskID: emptystr,
				Tag:    emptystr,
			},
		},
	},
	{
		2,
		cmdFromThrift,
		cmdToThrift,
		&worker.RunCommand{
			Argv:       someCmd,
			Env:        someEnv,
			SnapshotId: &nonemptystr,
			TimeoutMs:  &nonzero,
			JobId:      &emptystr,
			TaskId:     &emptystr,
			Tag:        &nonemptystr,
		},
		&runner.Command{
			Argv:       someCmd,
			EnvVars:    someEnv,
			SnapshotID: nonemptystr,
			Timeout:    time.Duration(nonzero) * time.Millisecond,
			LogTags: tags.LogTags{
				JobID:  emptystr,
				TaskID: emptystr,
				Tag:    nonemptystr,
			},
		},
	},
	{
		3,
		cmdFromThrift,
		cmdToThrift,
		&worker.RunCommand{
			Argv:       []string{},
			Env:        map[string]string{},
			SnapshotId: &emptystr,
			TimeoutMs:  &zero,
			JobId:      &emptystr,
			TaskId:     &emptystr,
			Tag:        &emptystr,
		},
		&runner.Command{
			Argv:    []string{},
			EnvVars: map[string]string{},
			Timeout: time.Duration(zero),
		},
	},

	//RunStatus
	{
		4,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_UNKNOWN,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
			JobId:    nil,
			TaskId:   nil,
			Tag:      nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.UNKNOWN,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		5,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_PENDING,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.PENDING,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		6,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_RUNNING,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.RUNNING,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		7,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_COMPLETE,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil},
		runner.RunStatus{
			RunID:     "",
			State:     runner.COMPLETE,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		8,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:     worker.Status_COMPLETE,
			RunId:      "",
			OutUri:     nil,
			ErrUri:     nil,
			Error:      nil,
			ExitCode:   nil,
			SnapshotId: &deadbeefID,
		},
		runner.RunStatus{
			RunID:      "",
			State:      runner.COMPLETE,
			StdoutRef:  "",
			StderrRef:  "",
			ExitCode:   0,
			Error:      "",
			SnapshotID: deadbeefID,
		},
	},
	{
		9,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_FAILED,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.FAILED,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		10,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_ABORTED,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.ABORTED,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		11,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_TIMEDOUT,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil,
		},
		runner.RunStatus{
			RunID:     "",
			State:     runner.TIMEDOUT,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		12,
		rsFromThrift,
		nil,
		&worker.RunStatus{
			Status:   worker.Status_BADREQUEST,
			RunId:    "",
			OutUri:   nil,
			ErrUri:   nil,
			Error:    nil,
			ExitCode: nil},
		runner.RunStatus{
			RunID:     "",
			State:     runner.BADREQUEST,
			StdoutRef: "",
			StderrRef: "",
			ExitCode:  0,
			Error:     "",
		},
	},
	{
		13,
		rsFromThrift,
		rsToThrift,
		&worker.RunStatus{
			Status:   worker.Status_BADREQUEST,
			RunId:    "id",
			OutUri:   &nonemptystr,
			ErrUri:   &nonemptystr,
			Error:    &nonemptystr,
			ExitCode: &nonzero,
		},
		runner.RunStatus{
			RunID:     "id",
			State:     runner.BADREQUEST,
			StdoutRef: nonemptystr,
			StderrRef: nonemptystr,
			ExitCode:  int(nonzero),
			Error:     nonemptystr,
		},
	},

	//WorkerStatus
	{
		14,
		wsFromThrift,
		nil,
		&worker.WorkerStatus{
			Runs: nil,
		},
		WorkerStatus{
			Runs: []runner.RunStatus{},
		},
	},
	{
		15,
		wsFromThrift,
		wsToThrift,
		&worker.WorkerStatus{
			Runs: []*worker.RunStatus{
				&worker.RunStatus{
					Status:   worker.Status_PENDING,
					RunId:    "id",
					OutUri:   &nonemptystr,
					ErrUri:   &nonemptystr,
					Error:    &nonemptystr,
					ExitCode: &nonzero},
			},
		},
		WorkerStatus{
			Runs: []runner.RunStatus{
				runner.RunStatus{
					RunID:     "id",
					State:     runner.PENDING,
					StdoutRef: nonemptystr,
					StderrRef: nonemptystr,
					ExitCode:  int(nonzero),
					Error:     nonemptystr,
				},
			},
		},
	},
}

func TestTranslation(t *testing.T) {
	for _, test := range tests {
		if domain := test.thriftToDomain(test.thrift); !reflect.DeepEqual(domain, test.domain) {
			t.Fatalf("%d expected:\n%v; was:\n%v", test.id, test.domain, domain)
		}
		if test.domainToThrift != nil {
			if thrift := test.domainToThrift(test.domain); !reflect.DeepEqual(thrift, test.thrift) {
				t.Fatalf("%d expected:\n%v; was:\n%v", test.id, test.thrift, thrift)
			}
		}
	}
}
