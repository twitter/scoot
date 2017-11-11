package cas

import (
	"bytes"
	"testing"

	"golang.org/x/net/context"
	googlebytestream "google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/snapshot/store"
)

func TestFindMissingBlobsStub(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	// Create 2 digests, write 1 to Store, check both for missing, expect other 1 back
	dExists := &remoteexecution.Digest{Hash: "abc123", SizeBytes: 1}
	dMissing := &remoteexecution.Digest{Hash: "efg456", SizeBytes: 9}
	digests := []*remoteexecution.Digest{dExists, dMissing}
	expected := []*remoteexecution.Digest{dMissing}

	resourceName := bazel.DigestStoreName(dExists)
	err := f.Write(resourceName, bytes.NewReader([]byte("")), nil)
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	ctx := context.Background()
	req := remoteexecution.FindMissingBlobsRequest{BlobDigests: digests}

	res, err := s.FindMissingBlobs(ctx, &req)
	if err != nil {
		t.Fatalf("Error response from FindMissingBlobs: %v", err)
	}

	if len(expected) != len(res.MissingBlobDigests) {
		t.Fatalf("Length of missing blobs mismatch, expected %d got %d", len(expected), len(res.MissingBlobDigests))
	}
	for i, d := range res.MissingBlobDigests {
		if expected[i] != d {
			t.Errorf("Non-match iterating through missing digests, expected %s got: %s", expected[i], d)
		}
	}
}

func TestBatchUpdateBlobsStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := remoteexecution.BatchUpdateBlobsRequest{}

	_, err := s.BatchUpdateBlobs(ctx, &req)
	if err == nil {
		t.Fatalf("Non-error response from BatchUpdateBlobs")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

func TestGetTree(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := remoteexecution.GetTreeRequest{}

	_, err := s.GetTree(ctx, &req)
	if err == nil {
		t.Fatalf("Non-error response from GetTree")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

// TODO more real tests (data read is the same, different offset/limit tests, exceed max buffer, negative tests)
func TestRead(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	// Write a resource to underlying store
	d := &remoteexecution.Digest{Hash: "01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b", SizeBytes: 1}
	resourceName := bazel.DigestStoreName(d)
	err := f.Write(resourceName, bytes.NewReader([]byte("")), nil)
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	req := googlebytestream.ReadRequest{ResourceName: "blobs/01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b/1", ReadOffset: 0, ReadLimit: 0}
	n := nilReadServer{}

	err = s.Read(&req, &n)
	if err != nil {
		t.Errorf("Error response from Read: %v", err)
	}
}

func TestWriteStub(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	n := nilWriteServer{}

	err := s.Write(&n)
	if err != nil {
		t.Errorf("Error response from Write: %v", err)
	}
}

func TestQueryWriteStatusStub(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	ctx := context.Background()
	req := googlebytestream.QueryWriteStatusRequest{}

	res, err := s.QueryWriteStatus(ctx, &req)
	if err != nil {
		t.Fatalf("Error response from QueryWriteStatus: %v", err)
	}

	if !res.Complete {
		t.Errorf("Unexpected false returned from Complete")
	}
}

// Implements googlebytestream.ByteStream_ReadServer interface
type nilReadServer struct{ grpc.ServerStream }

func (s *nilReadServer) Send(*googlebytestream.ReadResponse) error { return nil }

// Implements googlebytestream.ByteStream_WriteServer interface
type nilWriteServer struct{ grpc.ServerStream }

func (s *nilWriteServer) SendAndClose(*googlebytestream.WriteResponse) error { return nil }
func (s *nilWriteServer) Recv() (*googlebytestream.WriteRequest, error)      { return nil, nil }
