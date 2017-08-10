package sched

import (
	"github.com/twitter/scoot/common/thrifthelpers"
	"github.com/twitter/scoot/sched/gen-go/schedthrift"
	"testing"
)

func Test_DeserializeJob_BadData(t *testing.T) {
	job, err := DeserializeJob([]byte{0, 1, 2, 3})

	if err == nil {
		t.Error("Expected job deserialization to fail with an error")
	}

	if job != nil {
		t.Errorf("Expected Returned job to be nil when deserialization fails not %+v", job)
	}
}

func Test_DeserializeJob_MinThrift(t *testing.T) {
	thriftJob := schedthrift.NewJob()
	thriftJob.ID = "123"
	thriftJob.JobDefinition = schedthrift.NewJobDefinition()

	// ensure our idea of min job matches the thrift spec
	binaryJob, err := thrifthelpers.BinarySerialize(thriftJob)
	if err != nil {
		t.Errorf("unexpected error serializing minJob %+v", err)
	}

	// ensure we can covert this to a scheduler job
	if _, err := DeserializeJob(binaryJob); err != nil {
		t.Errorf("unexpected error converting to Scheduler Job %+v", err)
	}
}
