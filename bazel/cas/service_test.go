package cas

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	uuid "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/snapshot/store"
)

var testHash1 string = "ce58a4479be1d32816ee82e57eae04415dc2bda173fa7b0f11d18aa67856f242"
var testSize1 int64 = 7
var testData1 []byte = []byte("abc1234")

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
	req := &remoteexecution.FindMissingBlobsRequest{BlobDigests: digests}

	res, err := s.FindMissingBlobs(ctx, req)
	if err != nil {
		t.Fatalf("Error response from FindMissingBlobs: %v", err)
	}

	if len(expected) != len(res.MissingBlobDigests) {
		t.Fatalf("Length of missing blobs mismatch, expected %d got %d", len(expected), len(res.MissingBlobDigests))
	}
	for i, d := range res.MissingBlobDigests {
		if expected[i] != d {
			t.Fatalf("Non-match iterating through missing digests, expected %s got: %s", expected[i], d)
		}
	}
}

func TestRead(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	// Write a resource to underlying store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	resourceName := bazel.DigestStoreName(d)
	err := f.Write(resourceName, bytes.NewReader(testData1), nil)
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	// Make a ReadRequest that exercises read limits and offsets
	offset, limit := int64(2), int64(2)
	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", testHash1, testSize1), ReadOffset: offset, ReadLimit: limit}
	r := makeFakeReadServer()

	// Make actual Read request
	err = s.Read(req, r)
	if err != nil {
		t.Fatalf("Error response from Read: %v", err)
	}

	// Get data sent/captured by fake server, and compare with expected based on testdata, limit, and offset
	b, err := ioutil.ReadAll(r.buffer)
	if err != nil {
		t.Fatalf("Error reading from fake server data: %v", err)
	}
	if bytes.Compare(b, testData1[offset:]) != 0 {
		t.Fatalf("Data read from fake server did not match - expected: %s, got: %s", testData1[offset:], b)
	}
	sends := int((testSize1-offset)/limit + (testSize1-offset)%limit)
	if r.sendCount != sends {
		t.Fatalf("Fake server Send() count mismatch - expected %d times based on - data len: %d ReadOffset: %d ReadLimit %d. got: %d", sends, testSize1, offset, limit, r.sendCount)
	}
	r.reset()
}

func TestWrite(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	w := makeFakeWriteServer(testHash1, testSize1, testData1, 3)

	// Make Write request with test data
	err := s.Write(w)
	if err != nil {
		t.Fatalf("Error response from Write: %v", err)
	}

	// Verify that fake write server was invoked as expected
	if w.committedSize != testSize1 {
		t.Fatalf("Size committed to fake server did not match - expected: %d, got: %d", testSize1, w.committedSize)
	}
	if w.recvCount != w.recvChunks {
		t.Fatalf("Number of write chunks to fake server did not match - expected: %d, got: %d", w.recvChunks, w.recvCount)
	}

	// Verify Write by reading directly from underlying Store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	resourceName := bazel.DigestStoreName(d)
	r, err := f.OpenForRead(resourceName)
	if err != nil {
		t.Fatalf("Failed to open expected resource for reading: %s: %v", resourceName, err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("Error reading from fake store: %v", err)
	}
	if bytes.Compare(b, testData1) != 0 {
		t.Fatalf("Data read from store did not match - expected: %s, got: %s", testData1, b)
	}
}

func TestWriteExisting(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	// Pre-write data directly to underlying Store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}

	resourceName := bazel.DigestStoreName(d)
	err := f.Write(resourceName, bytes.NewReader(testData1), nil)
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	w := makeFakeWriteServer(testHash1, testSize1, testData1, 3)

	// Make Write request with test data matching pre-written data
	err = s.Write(w)
	if err != nil {
		t.Fatalf("Error response from Write: %v", err)
	}

	// Verify that fake write server was invoked as expected - recv was called only once
	if w.committedSize != testSize1 {
		t.Fatalf("Size committed to fake server did not match - expected: %d, got: %d", testSize1, w.committedSize)
	}
	if w.recvCount != 1 {
		t.Fatalf("Number of write chunks to fake server did not match - expected: %d, got: %d", 1, w.recvCount)
	}
}

func TestQueryWriteStatusStub(t *testing.T) {
	f := &store.FakeStore{}
	s := casServer{storeConfig: &store.StoreConfig{Store: f}}

	ctx := context.Background()
	req := &bytestream.QueryWriteStatusRequest{}

	_, err := s.QueryWriteStatus(ctx, req)
	if err == nil {
		t.Fatal("Expected error response from QueryWriteStatus, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

func TestBatchUpdateBlobsStub(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := &remoteexecution.BatchUpdateBlobsRequest{}

	_, err := s.BatchUpdateBlobs(ctx, req)
	if err == nil {
		t.Fatalf("Non-error response from BatchUpdateBlobs")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

func TestGetTree(t *testing.T) {
	s := casServer{}
	ctx := context.Background()
	req := &remoteexecution.GetTreeRequest{}

	_, err := s.GetTree(ctx, req)
	if err == nil {
		t.Fatalf("Non-error response from GetTree")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("Not ok reading grpc status from error")
	}
	if st.Code() != codes.Unimplemented {
		t.Fatalf("Expected status code %d, got: %d", codes.Unimplemented, st.Code())
	}
}

// Fake Read/Write Servers

// Read server that fakes sending data to a client.
// 	sendCount - tracks times the server invokes Send for verification
// Implements bytestream.ByteStream_ReadServer interface
type fakeReadServer struct {
	buffer    *bytes.Buffer
	sendCount int
	grpc.ServerStream
}

func makeFakeReadServer() *fakeReadServer {
	return &fakeReadServer{
		buffer: new(bytes.Buffer),
	}
}

func (s *fakeReadServer) Send(r *bytestream.ReadResponse) error {
	s.buffer.Write(r.GetData())
	s.sendCount++
	return nil
}

func (s *fakeReadServer) reset() {
	s.buffer.Reset()
	s.sendCount = 0
}

// Write server that fakes receiving specified data from a client.
// One instance will fake-write one piece of data identified by Digest(hash, size),
// []byte data. Chunks supports Recv'ing data in chunks to exercise the CAS server.
//	recvCount - tracks times the server invokes Recv for verification
//	committedSize - tracks the total data len Recv'd by the server for verification
// Implements bytestream.ByteStream_WriteServer interface
type fakeWriteServer struct {
	resourceName  string
	data          []byte
	recvChunks    int
	recvCount     int
	offset        int64
	committedSize int64
	grpc.ServerStream
}

func makeFakeWriteServer(hash string, size int64, data []byte, chunks int) *fakeWriteServer {
	uid, _ := uuid.NewV4()
	return &fakeWriteServer{
		resourceName:  fmt.Sprintf("uploads/%s/blobs/%s/%d", uid, hash, size),
		data:          data,
		recvChunks:    chunks,
		offset:        0,
		committedSize: 0,
	}
}

func (s *fakeWriteServer) SendAndClose(res *bytestream.WriteResponse) error {
	s.committedSize = res.GetCommittedSize()
	return nil
}

func (s *fakeWriteServer) Recv() (*bytestream.WriteRequest, error) {
	// Format a WriteRequest based on the chunks requested and the offset of what has been recvd
	recvLen := int64(len(s.data) / s.recvChunks)
	if s.recvCount+1 >= s.recvChunks {
		recvLen = int64(len(s.data)) - s.offset
	}
	finished := false
	if recvLen+s.offset >= int64(len(s.data)) {
		finished = true
	}
	r := &bytestream.WriteRequest{
		ResourceName: s.resourceName,
		WriteOffset:  0,
		FinishWrite:  finished,
		Data:         s.data[s.offset : s.offset+recvLen],
	}
	s.offset = s.offset + recvLen
	s.recvCount++
	return r, nil
}
