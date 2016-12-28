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
var fortyEight = int64(48)
var trueVar = true

var cmdFromThrift = func(x interface{}) interface{} { return ThriftRunCommandToDomain(x.(*worker.RunCommand)) }
var cmdToThrift = func(x interface{}) interface{} { return DomainRunCommandToThrift(x.(*runner.Command)) }
var rsFromThrift = func(x interface{}) interface{} { return ThriftRunStatusToDomain(x.(*worker.RunStatus)) }
var rsToThrift = func(x interface{}) interface{} { return DomainRunStatusToThrift(x.(runner.RunStatus)) }
var rssFromThrift = func(x interface{}) interface{} {
	st, err := ThriftRunStatusesToDomain(x.([]*worker.RunStatus))
	if err != nil {
		panic(err)
	}
	return st
}
var rssToThrift = func(x interface{}) interface{} { return DomainRunStatusesToThrift(x.([]runner.RunStatus)) }
var wsFromThrift = func(x interface{}) interface{} { return ThriftWorkerStatusToDomain(x.(*worker.WorkerStatus)) }
var wsToThrift = func(x interface{}) interface{} { return DomainWorkerStatusToThrift(x.(WorkerStatus)) }
var rqFromThrift = func(x interface{}) interface{} { return ThriftRunQueryToDomain(*(x.(*worker.RunsQuery))) }
var rqToThrift = func(x interface{}) interface{} { return DomainRunQueryToThrift(x.(runner.Query)) }

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
			Timeout: time.Duration(nonzero) * time.Millisecond, SnapshotID: nonemptystr},
	},

	//RunStatus
	{
		3, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_UNKNOWN, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.UNKNOWN,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		4, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_PENDING, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.PENDING,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		5, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_RUNNING, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.RUNNING,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		6, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_COMPLETE, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.COMPLETE,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		7, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_FAILED, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.FAILED,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		8, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_ABORTED, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.ABORTED,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		9, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_TIMEDOUT, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.TIMEDOUT,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		10, rsFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		runner.RunStatus{RunID: "", State: runner.BADREQUEST,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		11, rsFromThrift, rsToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &emptystr, ErrUri: &emptystr, Error: &emptystr, ExitCode: &nonzero},
		runner.RunStatus{RunID: "id", State: runner.BADREQUEST,
			StdoutRef: emptystr, StderrRef: emptystr, ExitCode: int(nonzero), Error: emptystr},
	},
	{
		12, rsFromThrift, rsToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &nonemptystr, ErrUri: &nonemptystr, Error: &nonemptystr, ExitCode: &nonzero},
		runner.RunStatus{RunID: "id", State: runner.BADREQUEST,
			StdoutRef: nonemptystr, StderrRef: nonemptystr, ExitCode: int(nonzero), Error: nonemptystr},
	},

	//WorkerStatus
	{
		13, wsFromThrift, nil,
		&worker.WorkerStatus{Runs: nil},
		WorkerStatus{Runs: []runner.RunStatus{}},
	},
	{
		14, wsFromThrift, wsToThrift,
		&worker.WorkerStatus{Runs: []*worker.RunStatus{
			&worker.RunStatus{Status: worker.Status_PENDING, RunId: "id",
				OutUri: &nonemptystr, ErrUri: &nonemptystr, Error: &nonemptystr, ExitCode: &nonzero},
		}},
		WorkerStatus{Runs: []runner.RunStatus{
			runner.RunStatus{RunID: "id", State: runner.PENDING,
				StdoutRef: nonemptystr, StderrRef: nonemptystr, ExitCode: int(nonzero), Error: nonemptystr},
		}},
	},

	// RunStatuses
	{
		15, rssFromThrift, nil,
		[]*worker.RunStatus{
			&worker.RunStatus{Status: worker.Status_COMPLETE, RunId: "a",
				OutUri: nil, ErrUri: nil, Error: nil, ExitCode: &zero},
			&worker.RunStatus{Status: worker.Status_COMPLETE, RunId: "b",
				OutUri: nil, ErrUri: nil, Error: nil, ExitCode: &zero},
		},
		[]runner.RunStatus{
			runner.RunStatus{RunID: "a", State: runner.COMPLETE,
				StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
			runner.RunStatus{RunID: "b", State: runner.COMPLETE,
				StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
		},
	},

	// RunQuery
	{
		16, rqFromThrift, rqToThrift,
		&worker.RunsQuery{
			RunIds:    []string{"1", "2"},
			StateMask: &fortyEight,
		},
		runner.Query{
			Runs:   []runner.RunID{runner.RunID("1"), runner.RunID("2")},
			States: runner.StateMask(48),
		},
	},
	{
		17, rqFromThrift, rqToThrift,
		&worker.RunsQuery{
			AllRuns:   &trueVar,
			StateMask: &fortyEight,
		},
		runner.Query{
			AllRuns: true,
			States:  runner.StateMask(48),
		},
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
