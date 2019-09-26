package cas

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/apiserver/snapshot/store"
	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/allocator"
	"github.com/twitter/scoot/common/stats"
)

var testHash1 string = "36f583dd16f4e1e201eb1e6f6d8e35a2ccb3bbe2658de46b4ffae7b0e9ed872e"
var testSize1 int64 = 7
var testData1 []byte = []byte("abc1234")
var testHash2 string = "ed1073e458620413772fcf13e3730a6392211f04b63dde663d085eb72435d56b"
var testSize2 int64 = 7
var testData2 []byte = []byte("efg9876")
var testHash3 string = "67a619457aae3e869af3e7c92078424a773397c1520a9cec76fde54ee8350137"
var testSize3 int64 = 7
var testData3 []byte = []byte("qqqqqqq")
var testHash4 string = "40011c325e5305a8aaab651bb4328480576bd14c2186500ca49f5c171fbe916d"
var testSize4 int64 = 7
var testData4 []byte = []byte("wwwwwww")
var testHash5 string = "3de47205e772b39d369b811a8cc515a31cf310511bf1f4529b7234988509da55"
var testSize5 int64 = 7
var testData5 []byte = []byte("eeeeeee")
var testHash6 string = "5252f52cb79e2276783cfdca50304fed06a0eabc8dbcc3abfe3aaac5792c4fc6"
var testSize6 int64 = 7
var testData6 []byte = []byte("rrrrrrr")

// TODO make batch tests easier to programmatically test above BatchParallelism threshold

func TestFindMissingBlobs(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Create 6 digests, write 1 to Store, expect all missing expect 1
	// NOTE Count of 6 intended to be > cas.BatchParallelism value
	dExists := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	dMissing1 := &remoteexecution.Digest{Hash: testHash2, SizeBytes: testSize2}
	dMissing2 := &remoteexecution.Digest{Hash: testHash3, SizeBytes: testSize3}
	dMissing3 := &remoteexecution.Digest{Hash: testHash4, SizeBytes: testSize4}
	dMissing4 := &remoteexecution.Digest{Hash: testHash5, SizeBytes: testSize5}
	dMissing5 := &remoteexecution.Digest{Hash: testHash6, SizeBytes: testSize6}
	digests := []*remoteexecution.Digest{dExists, dMissing1, dMissing2, dMissing3, dMissing4, dMissing5}
	expected := []*remoteexecution.Digest{dMissing1, dMissing2, dMissing3, dMissing4, dMissing5}

	resourceName := bazel.DigestStoreName(dExists)
	buf := ioutil.NopCloser(bytes.NewReader([]byte("")))
	err := f.Write(resourceName, store.NewResource(buf, 0, nil))
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	req := &remoteexecution.FindMissingBlobsRequest{BlobDigests: digests}

	res, err := s.FindMissingBlobs(context.Background(), req)
	if err != nil {
		t.Fatalf("Error response from FindMissingBlobs: %v", err)
	}

	if len(expected) != len(res.MissingBlobDigests) {
		t.Fatalf("Length of missing blobs mismatch, expected %d got %d", len(expected), len(res.MissingBlobDigests))
	}
	for _, d := range expected {
		found := false
		for _, v := range res.MissingBlobDigests {
			if v == d {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected missing digest %s not found in results", d)
		}
	}
}

func TestRead(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Write a resource to underlying store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	resourceName := bazel.DigestStoreName(d)
	buf := ioutil.NopCloser(bytes.NewReader(testData1))
	err := f.Write(resourceName, store.NewResource(buf, testSize1, nil))
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

func TestReadEmpty(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Note: don't actually write the underlying resource beforehand. We expect
	// that reading an empty blob will bypass the underlying store

	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", bazel.EmptySha, bazel.EmptySize), ReadOffset: int64(0), ReadLimit: int64(0)}
	r := makeFakeReadServer()

	// Make actual Read request
	err := s.Read(req, r)
	if err != nil {
		t.Fatalf("Error response from Read: %v", err)
	}

	// Get data sent/captured by fake server, and compare with expected based on testdata, limit, and offset
	b, err := ioutil.ReadAll(r.buffer)
	if err != nil {
		t.Fatalf("Error reading from fake server data: %v", err)
	}
	if bytes.Compare(b, []byte{}) != 0 {
		t.Fatalf("Data read from fake server did not match - expected: %s, got: %s", []byte{}, b)
	}
	r.reset()
}

func TestReadMissing(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Note: don't actually write the underlying resource beforehand. We expect
	// that reading an empty blob will bypass the underlying store

	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", testHash1, testSize1)}
	r := makeFakeReadServer()

	// Make actual Read request
	err := s.Read(req, r)
	if err == nil {
		t.Fatal("Unexpected success - want NotFound error response from Read")
	}
	c := status.Code(err)
	if c != codes.NotFound {
		t.Fatalf("Status code from error not expected: %s, got: %s", codes.NotFound, c)
	}

	r.reset()
}

func TestWrite(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

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
	_, err = readAndCompare(f, resourceName, testData1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWriteEmpty(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	w := makeFakeWriteServer(bazel.EmptySha, bazel.EmptySize, []byte{}, 1)

	// Make Write request with test data
	err := s.Write(w)
	if err != nil {
		t.Fatalf("Error response from Write: %v", err)
	}

	// Verify that fake write server was invoked as expected
	if w.committedSize != bazel.EmptySize {
		t.Fatalf("Size committed to fake server did not match - expected: %d, got: %d", bazel.EmptySize, w.committedSize)
	}
	if w.recvCount != w.recvChunks {
		t.Fatalf("Number of write chunks to fake server did not match - expected: %d, got: %d", w.recvChunks, w.recvCount)
	}
	// Don't verify - we reserve the right to not actually write to the underlying store in this scenario
}

func TestWriteExisting(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Pre-write data directly to underlying Store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}

	resourceName := bazel.DigestStoreName(d)
	buf := ioutil.NopCloser(bytes.NewReader(testData1))
	err := f.Write(resourceName, store.NewResource(buf, testSize1, nil))
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
	s := makeCasFromStore(f)

	req := &bytestream.QueryWriteStatusRequest{}

	_, err := s.QueryWriteStatus(context.Background(), req)
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

func TestBatchUpdateBlobs(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// All expected to succeeded unless otherwise indicated.
	// NOTE Count of 6 intended to be > cas.BatchParallelism value
	req := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1},
				Data:   testData1,
			},
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: testHash2, SizeBytes: testSize2},
				Data:   testData2,
			},
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: testHash3, SizeBytes: testSize3},
				Data:   testData3,
			},
			// empty (should still succeed)
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: bazel.EmptySha, SizeBytes: bazel.EmptySize},
				Data:   []byte{},
			},
			// mismatch data sha
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: testHash4, SizeBytes: testSize4},
				Data:   testData2,
			},
			// mismatch data length
			&remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest: &remoteexecution.Digest{Hash: testHash5, SizeBytes: testSize5 + 1},
				Data:   testData5,
			},
		},
	}

	res, err := s.BatchUpdateBlobs(context.Background(), req)
	if err != nil {
		t.Fatalf("Error response from BatchUpdateBlobs: %s", err)
	}

	// Check response validity in Status and/or underlying Store as required
	for _, writeRes := range res.GetResponses() {
		switch h := writeRes.GetDigest().GetHash(); h {
		case testHash1:
			if writeRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			resourceName := bazel.DigestStoreName(&remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1})
			_, err = readAndCompare(f, resourceName, testData1)
			if err != nil {
				t.Fatal(err)
			}
		case testHash2:
			if writeRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			resourceName := bazel.DigestStoreName(&remoteexecution.Digest{Hash: testHash2, SizeBytes: testSize2})
			_, err = readAndCompare(f, resourceName, testData2)
			if err != nil {
				t.Fatal(err)
			}
		case testHash3:
			if writeRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			resourceName := bazel.DigestStoreName(&remoteexecution.Digest{Hash: testHash3, SizeBytes: testSize3})
			_, err = readAndCompare(f, resourceName, testData3)
			if err != nil {
				t.Fatal(err)
			}
		case testHash4:
			if writeRes.Status.Code != int32(google_rpc_code.Code_INVALID_ARGUMENT) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_INVALID_ARGUMENT)
			}
		case testHash5:
			if writeRes.Status.Code != int32(google_rpc_code.Code_INVALID_ARGUMENT) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_INVALID_ARGUMENT)
			}
		case bazel.EmptySha:
			if writeRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", writeRes.Status.Code, h, google_rpc_code.Code_OK)
			}
		default:
			t.Fatalf("Unexpected hash in response: %s", h)
		}
	}
}

func TestBatchReadBlobs(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// All expected to succeed unless otherwise indicated.
	// NOTE Count of 6 intended to be > cas.BatchParallelism value
	req := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			&remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1},
			&remoteexecution.Digest{Hash: testHash2, SizeBytes: testSize2},
			&remoteexecution.Digest{Hash: testHash3, SizeBytes: testSize3},
			// empty (should still succeed)
			&remoteexecution.Digest{Hash: bazel.EmptySha, SizeBytes: bazel.EmptySize},
			// mismatch length
			&remoteexecution.Digest{Hash: testHash4, SizeBytes: testSize4 + 1},
			// non exist
			&remoteexecution.Digest{Hash: testHash5, SizeBytes: testSize5},
		},
	}

	// write expected data to underlying store
	writeHashes := []string{testHash1, testHash2, testHash3, testHash4}
	writeSizes := []int64{testSize1, testSize2, testSize3, testSize4}
	writeData := [][]byte{testData1, testData2, testData3, testData4}
	for i := 0; i < len(writeHashes); i++ {
		d := &remoteexecution.Digest{Hash: writeHashes[i], SizeBytes: writeSizes[i]}
		resourceName := bazel.DigestStoreName(d)
		buf := ioutil.NopCloser(bytes.NewReader(writeData[i]))
		err := f.Write(resourceName, store.NewResource(buf, writeSizes[i], nil))
		if err != nil {
			t.Fatalf("Failed to write into FakeStore: %v", err)
		}
	}

	res, err := s.BatchReadBlobs(context.Background(), req)
	if err != nil {
		t.Fatalf("Non-nil err response from BatchReadBlobs: %s", err)
	}

	// Check response validity in Status and/or Data as required
	for _, readRes := range res.GetResponses() {
		switch h := readRes.GetDigest().GetHash(); h {
		case testHash1:
			if readRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			if bytes.Compare(readRes.Data, testData1) != 0 {
				t.Fatalf("Data read did not match - expected: %s, got: %s", testData1, readRes.Data)
			}
		case testHash2:
			if readRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			if bytes.Compare(readRes.Data, testData2) != 0 {
				t.Fatalf("Data read did not match - expected: %s, got: %s", testData2, readRes.Data)
			}
		case testHash3:
			if readRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_OK)
			}
			if bytes.Compare(readRes.Data, testData3) != 0 {
				t.Fatalf("Data read did not match - expected: %s, got: %s", testData3, readRes.Data)
			}
		case testHash4:
			if readRes.Status.Code != int32(google_rpc_code.Code_INVALID_ARGUMENT) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_INVALID_ARGUMENT)
			}
		case testHash5:
			if readRes.Status.Code != int32(google_rpc_code.Code_NOT_FOUND) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_NOT_FOUND)
			}
		case bazel.EmptySha:
			if readRes.Status.Code != int32(google_rpc_code.Code_OK) {
				t.Fatalf("Unexpected status code %d for hash %s: %d", readRes.Status.Code, h, google_rpc_code.Code_OK)
			}
		default:
			t.Fatalf("Unexpected hash in response: %s", h)
		}
	}
}

func TestGetTreeStub(t *testing.T) {
	s := makeCasFromStore(&store.FakeStore{})
	req := &remoteexecution.GetTreeRequest{}
	gtServer := &fakeGetTreeServer{}

	err := s.GetTree(req, gtServer)
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

func TestMakeResultAddress(t *testing.T) {
	ad := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	// e.g. `echo -n "<testHash1>-<ResultAddressKey>" | shasum -a 256`
	knownHash := "96da053bc6583a4307c182fefa3ca7d55cfdfb3a770698adc197c61961b041f1"
	storeName := fmt.Sprintf("%s-%s.%s", bazel.StorePrefix, knownHash, bazel.StorePrefix)

	resultAddr, err := makeCacheResultAddress(ad)
	if err != nil {
		t.Fatalf("Failed to create cache result address: %v", err)
	}
	if resultAddr.storeName != storeName {
		t.Fatalf("Unexpected resulting store name: %s, want: %s", resultAddr.storeName, storeName)
	}
}

func TestGetActionResult(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	arAsBytes, err := getFakeActionResult()
	if err != nil {
		t.Fatalf("Error getting ActionResult: %s", err)
	}

	// Get ActionDigest and Write AR to underlying store using our result cache addressing convention
	ad := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	address, err := makeCacheResultAddress(ad)
	if err != nil {
		t.Fatalf("Failed to create cache result adress: %v", err)
	}
	buf := ioutil.NopCloser(bytes.NewReader(arAsBytes))
	err = f.Write(address.storeName, store.NewResource(buf, int64(len(arAsBytes)), nil))
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	// Make GetActionResult request
	req := &remoteexecution.GetActionResultRequest{ActionDigest: ad}

	resAr, err := s.GetActionResult(context.Background(), req)
	if err != nil {
		t.Fatalf("Error from GetActionResult: %v", err)
	}

	// Convert result to bytes and compare
	resAsBytes, err := proto.Marshal(resAr)
	if err != nil {
		t.Fatalf("Error serializing result: %s", err)
	}

	if bytes.Compare(arAsBytes, resAsBytes) != 0 {
		t.Fatal("Result not as expected after serialization")
	}
}

func TestGetActionResultMissing(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	// Make GetActionResult request
	req := &remoteexecution.GetActionResultRequest{
		ActionDigest: &remoteexecution.Digest{
			Hash:      testHash1,
			SizeBytes: testSize1,
		},
	}

	res, err := s.GetActionResult(context.Background(), req)
	if err == nil {
		t.Fatal("Unexpected success - want NotFound error response from GetActionResult")
	}
	if res != nil {
		t.Fatal("Unexpected non-nil GetActionResult")
	}
	c := status.Code(err)
	if c != codes.NotFound {
		t.Fatalf("Status code from error not expected: %s, got: %s", codes.NotFound, c)
	}
}

func TestUpdateActionResult(t *testing.T) {
	f := &store.FakeStore{}
	s := makeCasFromStore(f)

	rc := int32(42)
	ad := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	ar := &remoteexecution.ActionResult{ExitCode: rc}
	req := &remoteexecution.UpdateActionResultRequest{ActionDigest: ad, ActionResult: ar}

	_, err := s.UpdateActionResult(context.Background(), req)
	if err != nil {
		t.Fatalf("Error from UpdateActionResult: %v", err)
	}

	// Read from underlying store
	address, err := makeCacheResultAddress(ad)
	if err != nil {
		t.Fatalf("Failed to create cache result adress: %v", err)
	}

	b, err := readAndCompare(f, address.storeName, nil)
	if err != nil {
		t.Fatal(err)
	}
	resAr := &remoteexecution.ActionResult{}
	if err = proto.Unmarshal(b, resAr); err != nil {
		t.Fatalf("Error deserializing result: %s", err)
	}

	if resAr.GetExitCode() != rc {
		t.Fatalf("Result not as expected, got: %d, want: %d", resAr.GetExitCode(), rc)
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
	chunkSize := int64(len(s.data) / s.recvChunks)
	if s.recvCount+1 >= s.recvChunks {
		chunkSize = int64(len(s.data)) - s.offset
	}
	finished := false
	if chunkSize+s.offset >= int64(len(s.data)) {
		finished = true
	}
	r := &bytestream.WriteRequest{
		ResourceName: s.resourceName,
		WriteOffset:  s.offset,
		FinishWrite:  finished,
		Data:         s.data[s.offset : s.offset+chunkSize],
	}
	s.offset = s.offset + chunkSize
	s.recvCount++
	return r, nil
}

// Serialize an ActionResult for placement in a Store for use in ActionCache testing
func getFakeActionResult() ([]byte, error) {
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	ar := &remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			&remoteexecution.OutputFile{Path: "/dir/file", Digest: d},
		},
		OutputDirectories: []*remoteexecution.OutputDirectory{
			&remoteexecution.OutputDirectory{Path: "/dir", TreeDigest: d},
		},
		ExitCode:     int32(12),
		StdoutDigest: d,
		StderrDigest: d,
	}
	arAsBytes, err := proto.Marshal(ar)
	if err != nil {
		return nil, err
	}
	return arAsBytes, nil
}

// Fake GetTreeServer
// Implements ContentAddressableStorage_GetTreeServer interface
type fakeGetTreeServer struct {
	grpc.ServerStream
}

func (s *fakeGetTreeServer) Send(*remoteexecution.GetTreeResponse) error {
	return nil
}

// Helper functions

func readAndCompare(f store.Store, name string, testData []byte) ([]byte, error) {
	r, err := f.OpenForRead(name)
	if err != nil {
		return nil, fmt.Errorf("Failed to open expected resource for reading: %s: %v", name, err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("Error reading from fake store: %v", err)
	}
	if testData != nil {
		if bytes.Compare(b, testData) != 0 {
			return nil, fmt.Errorf("Data read from store did not match - expected: %s, got: %s", testData, b)
		}
	}
	return b, nil
}

func makeCasFromStore(s store.Store) casServer {
	a, _ := allocator.NewAbstractAllocator(10 * 1024 * 1024)
	return casServer{
		storeConfig: &store.StoreConfig{Store: s},
		stat:        stats.NilStatsReceiver(),
		alloc:       a,
	}
}
