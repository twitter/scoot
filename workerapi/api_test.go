package workerapi

import (
	"reflect"
	"testing"
	"time"

	"github.com/scootdev/scoot/runner"
	"github.com/scootdev/scoot/workerapi/gen-go/worker"
)

var someCmd = []string{"", "x"}
var someEnv = map[string]string{"a": "1", "b": "2"}
var zero = int32(0)
var nonzero = int32(12345)
var emptystr = ""
var nonemptystr = "abcdef"

var cmdFromThrift = func(x interface{}) interface{} { return ThriftRunCommandToDomain(x.(*worker.RunCommand)) }
var cmdToThrift = func(x interface{}) interface{} { return DomainRunCommandToThrift(x.(*runner.Command)) }
var rsFromThrift = func(x interface{}) interface{} { return ThriftRunStatusToDomain(x.(*worker.RunStatus)) }
var rsToThrift = func(x interface{}) interface{} { return DomainRunStatusToThrift(x.(runner.ProcessStatus)) }
var wsFromThrift = func(x interface{}) interface{} { return ThriftWorkerStatusToDomain(x.(*worker.WorkerStatus)) }
var wsToThrift = func(x interface{}) interface{} { return DomainWorkerStatusToThrift(x.(*WorkerStatus)) }

var tests = []struct {
	id             int
	thriftToDomain func(interface{}) interface{}
	domainToThrift func(interface{}) interface{}
	thrift         interface{}
	domain         interface{}
}{
	//Cmd
	{
		0, cmdFromThrift, nil,
		&worker.RunCommand{Argv: []string{}, Env: nil, SnapshotId: nil, TimeoutMs: nil},
		&runner.Command{Argv: []string{}, EnvVars: map[string]string{}, Timeout: time.Duration(zero)},
	},
	{
		1, cmdFromThrift, cmdToThrift,
		&worker.RunCommand{Argv: []string{}, Env: map[string]string{}, SnapshotId: &emptystr, TimeoutMs: &zero},
		&runner.Command{Argv: []string{}, EnvVars: map[string]string{}, Timeout: time.Duration(zero)},
	},
	{
		2, cmdFromThrift, cmdToThrift,
		&worker.RunCommand{Argv: someCmd, Env: someEnv, SnapshotId: &nonemptystr, TimeoutMs: &nonzero},
		&runner.Command{Argv: someCmd, EnvVars: someEnv,
			Timeout: time.Duration(nonzero) * time.Millisecond, SnapshotId: nonemptystr},
	},

	//RunStatus
	{
		3, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_UNKNOWN, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.UNKNOWN,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		4, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_PENDING, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.PENDING,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		5, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_RUNNING, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.RUNNING,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		6, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_COMPLETE, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.COMPLETE,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		7, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_FAILED, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.FAILED,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		8, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_ABORTED, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.ABORTED,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		9, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_TIMEDOUT, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.TIMEDOUT,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		10, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.ProcessStatus{RunId: "", State: runner.BADREQUEST,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		11, rsFromThrift, rsToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &emptystr, ErrUri: &emptystr, Error: &emptystr, ExitCode: &nonzero},
		runner.ProcessStatus{RunId: "id", State: runner.BADREQUEST,
			StdoutRef: emptystr, StderrRef: emptystr, ExitCode: int(nonzero), Error: emptystr},
	},
	{
		12, rsFromThrift, rsToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &nonemptystr, ErrUri: &nonemptystr, Error: &nonemptystr, ExitCode: &nonzero},
		runner.ProcessStatus{RunId: "id", State: runner.BADREQUEST,
			StdoutRef: nonemptystr, StderrRef: nonemptystr, ExitCode: int(nonzero), Error: nonemptystr},
	},

	//WorkerStatus
	{
		13, wsFromThrift, nil,
		&worker.WorkerStatus{Runs: nil, VersionId: nil},
		&WorkerStatus{Runs: []runner.ProcessStatus{}, VersionId: ""},
	},
	{
		14, wsFromThrift, wsToThrift,
		&worker.WorkerStatus{Runs: []*worker.RunStatus{
			&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
				OutUri: &nonemptystr, ErrUri: &nonemptystr, Error: &nonemptystr, ExitCode: &nonzero},
		}, VersionId: &nonemptystr},
		&WorkerStatus{Runs: []runner.ProcessStatus{
			runner.ProcessStatus{RunId: "id", State: runner.BADREQUEST,
				StdoutRef: nonemptystr, StderrRef: nonemptystr, ExitCode: int(nonzero), Error: nonemptystr},
		}, VersionId: nonemptystr},
	},
}

func TestTranslation(t *testing.T) {
	for _, test := range tests {
		if domain := test.thriftToDomain(test.thrift); !reflect.DeepEqual(domain, test.domain) {
			t.Fatalf("%d expected %v; was: %v", test.id, test.domain, domain)
		}
		if test.domainToThrift != nil {
			if thrift := test.domainToThrift(test.domain); !reflect.DeepEqual(thrift, test.thrift) {
				t.Fatalf("%d expected %v; was: %v", test.id, test.thrift, thrift)
			}
		}
	}
}
