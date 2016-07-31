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
var statusFromThrift = func(x interface{}) interface{} { return ThriftRunStatusToDomain(x.(*worker.RunStatus)) }
var statusToThrift = func(x interface{}) interface{} { return DomainRunStatusToThrift(x.(*runner.ProcessStatus)) }

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
		&worker.RunCommand{Argv: []string{}, Env: map[string]string{}, SnapshotId: nil, TimeoutMs: &zero},
		&runner.Command{Argv: []string{}, EnvVars: map[string]string{}, Timeout: time.Duration(zero)},
	},
	{
		2, cmdFromThrift, cmdToThrift,
		&worker.RunCommand{Argv: someCmd, Env: someEnv, SnapshotId: nil, TimeoutMs: &nonzero},
		&runner.Command{Argv: someCmd, EnvVars: someEnv, Timeout: time.Duration(nonzero) * time.Millisecond},
	},

	//Status
	{
		3, statusFromThrift, nil,
		&worker.RunStatus{Status: worker.Status_UNKNOWN, RunId: "",
			OutUri: nil, ErrUri: nil, Error: nil, ExitCode: nil},
		&runner.ProcessStatus{RunId: "", State: runner.UNKNOWN,
			StdoutRef: "", StderrRef: "", ExitCode: 0, Error: ""},
	},
	{
		4, statusFromThrift, statusToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &emptystr, ErrUri: &emptystr, Error: &emptystr, ExitCode: &nonzero},
		&runner.ProcessStatus{RunId: "id", State: runner.BADREQUEST,
			StdoutRef: emptystr, StderrRef: emptystr, ExitCode: int(nonzero), Error: emptystr},
	},
	{
		5, statusFromThrift, statusToThrift,
		&worker.RunStatus{Status: worker.Status_BADREQUEST, RunId: "id",
			OutUri: &nonemptystr, ErrUri: &nonemptystr, Error: &nonemptystr, ExitCode: &nonzero},
		&runner.ProcessStatus{RunId: "id", State: runner.BADREQUEST,
			StdoutRef: nonemptystr, StderrRef: nonemptystr, ExitCode: int(nonzero), Error: nonemptystr},
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
