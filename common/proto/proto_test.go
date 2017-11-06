package proto

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
)

func TestGetSha256(t *testing.T) {
	e := empty.Empty{}
	s, l, err := GetSha256(&e)
	if err != nil {
		t.Errorf("GetSha256 failure: %v", err)
	}
	// This is what sha-256'ing no data returns
	// This is as good as precomputing any old data structure, presumably
	nilDataSha := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if s != nilDataSha {
		t.Errorf("Expected known sha for nil/empty data %s, got: %s", nilDataSha, s)
	}
	if l != 0 {
		t.Errorf("Expected zero length data, got: %d", l)
	}
}
