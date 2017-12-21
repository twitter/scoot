package proto

import (
	"testing"

	"github.com/golang/protobuf/ptypes/duration"
	"github.com/golang/protobuf/ptypes/empty"
)

func TestGetSha256(t *testing.T) {
	e := empty.Empty{}
	s, l, err := GetSha256(&e)
	if err != nil {
		t.Fatalf("GetSha256 failure: %v", err)
	}
	// This is what sha-256'ing no data returns
	// This is as good as precomputing any old data structure, presumably
	nilDataSha := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if s != nilDataSha {
		t.Fatalf("Expected known sha for nil/empty data %s, got: %s", nilDataSha, s)
	}
	if l != 0 {
		t.Fatalf("Expected zero length data, got: %d", l)
	}
}

func TestMsDuration(t *testing.T) {
	d := duration.Duration{Seconds: 3, Nanos: 5000004}
	ms := GetMsFromDuration(&d)
	if ms != 3005 {
		t.Fatalf("Expected 3005, got: %dms", ms)
	}

	d = GetDurationFromMs(ms)
	if d.GetSeconds() != 3 || d.GetNanos() != 5000000 {
		t.Fatalf("Expected 3s 5000000ns, got: %ds %dns", d.GetSeconds(), d.GetNanos())
	}
}
