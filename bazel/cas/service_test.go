package cas

import (
	"testing"

	"golang.org/x/net/context"
	googlebytestream "google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFindMissingBlobsStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	digests := []*remoteexecution.Digest{&remoteexecution.Digest{Hash: "abc123", SizeBytes: 1}}
	req := remoteexecution.FindMissingBlobsRequest{BlobDigests: digests}

	res, err := s.FindMissingBlobs(ctx, &req)
	if err != nil {
		t.Errorf("Error response from FindMissingBlobs: %v", err)
	}

	if len(digests) != len(res.MissingBlobDigests) {
		t.Errorf("Length of missing blobs mismatch, expected %d got %d", len(digests), len(res.MissingBlobDigests))
	}
	for i, d := range res.MissingBlobDigests {
		if digests[i] != d {
			t.Errorf("Non match iterating through missing digests, expected %s got: %s", digests[i], d)
		}
	}
}

func TestBatchUpdateBlobsStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := remoteexecution.BatchUpdateBlobsRequest{}

	_, err := s.BatchUpdateBlobs(ctx, &req)
	if err == nil {
		t.Errorf("Non-error response from BatchUpdateBlobs")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Errorf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

func TestGetTreeStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := remoteexecution.GetTreeRequest{}

	_, err := s.GetTree(ctx, &req)
	if err == nil {
		t.Errorf("Non-error response from GetTree")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Errorf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

func TestReadStub(t *testing.T) {
	s := casServer{}
	req := googlebytestream.ReadRequest{}
	n := nilReadServer{}

	err := s.Read(&req, &n)
	if err != nil {
		t.Errorf("Error response from Read: %v", err)
	}
}

func TestWriteStub(t *testing.T) {
	s := casServer{}
	n := nilWriteServer{}

	err := s.Write(&n)
	if err != nil {
		t.Errorf("Error response from Write: %v", err)
	}
}

func TestQueryWriteStatusStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := googlebytestream.QueryWriteStatusRequest{}

	res, err := s.QueryWriteStatus(ctx, &req)
	if err != nil {
		t.Errorf("Error response from QueryWriteStatus: %v", err)
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
