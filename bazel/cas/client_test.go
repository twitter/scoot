package cas

//go:generate mockgen -destination=mock_bytestream/bsclient_mock.go google.golang.org/genproto/googleapis/bytestream ByteStreamClient,ByteStream_ReadClient,ByteStream_WriteClient
//go:generate mockgen -destination=mock_connection_api/connection_api_mock.go github.com/twitter/scoot/bazel/cas/connection-api GRPCDialer,ClientConnPtr

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/twitter/scoot/bazel/cas/connection-api"
	"math/rand"
	"testing"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/golang/mock/gomock"
	uuid "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas/mock_bytestream"
	"github.com/twitter/scoot/bazel/cas/mock_connection_api"
	"github.com/twitter/scoot/bazel/execution/mock_remoteexecution"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common/dialer"
)

func TestClientRead(t *testing.T) {
	// Make a ReadRequest with a known limit
	offset, limit := int64(0), testSize1
	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", testHash1, testSize1), ReadOffset: offset, ReadLimit: limit}

	mockCtrl := gomock.NewController(t)
	bsClientMock := mock_bytestream.NewMockByteStreamClient(mockCtrl)
	bsReadClientMock := mock_bytestream.NewMockByteStream_ReadClient(mockCtrl)

	bsClientMock.EXPECT().Read(context.Background(), req).Return(bsReadClientMock, nil)
	bsReadClientMock.EXPECT().Recv().Return(&bytestream.ReadResponse{Data: testData1}, nil)

	data, err := MakeCASClient().readFromClient(bsClientMock, req)
	if err != nil {
		t.Fatalf("Error from client read: %s", err)
	}
	if bytes.Compare(testData1, data) != 0 {
		t.Fatalf("Data read from client did not match - expected: %s, got: %s", testData1, data)
	}
}

func TestClientReadMissing(t *testing.T) {
	offset, limit := int64(0), testSize1
	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", testHash1, testSize1), ReadOffset: offset, ReadLimit: limit}

	mockCtrl := gomock.NewController(t)
	bsClientMock := mock_bytestream.NewMockByteStreamClient(mockCtrl)
	bsReadClientMock := mock_bytestream.NewMockByteStream_ReadClient(mockCtrl)

	bsClientMock.EXPECT().Read(context.Background(), req).Return(bsReadClientMock, nil)
	bsReadClientMock.EXPECT().Recv().Return(nil, status.Error(codes.NotFound, ""))

	data, err := MakeCASClient().readFromClient(bsClientMock, req)
	if err == nil {
		t.Fatal("Unexpected success from client read")
	}
	if data != nil {
		t.Fatal("Unexpected non-nil data from client read")
	}
	if !IsNotFoundError(err) {
		t.Fatalf("Expected NotFoundError, got: %v", err)
	}
}

func TestClientReadEmpty(t *testing.T) {
	digest := &remoteexecution.Digest{
		Hash:      bazel.EmptySha,
		SizeBytes: bazel.EmptySize,
	}
	data, err := MakeCASClient().ByteStreamRead(dialer.NewConstantResolver(""), digest, backoff.NewConstantBackOff(0))
	if data != nil || err != nil {
		t.Fatal("Expected nil data and err from empty client read")
	}
}

func TestClientWrite(t *testing.T) {
	// Make a WriteRequest with known data
	offset, limit := int64(0), testSize1
	uid, _ := uuid.NewV4()
	req := &bytestream.WriteRequest{ResourceName: fmt.Sprintf("%s/blobs/%s/%d", uid, testHash1, limit), WriteOffset: offset, FinishWrite: true, Data: testData1}

	mockCtrl := gomock.NewController(t)
	bsClientMock := mock_bytestream.NewMockByteStreamClient(mockCtrl)
	bsWriteClientMock := mock_bytestream.NewMockByteStream_WriteClient(mockCtrl)

	bsClientMock.EXPECT().Write(context.Background()).Return(bsWriteClientMock, nil)
	bsWriteClientMock.EXPECT().Send(req).Return(nil)
	bsWriteClientMock.EXPECT().CloseAndRecv().Return(&bytestream.WriteResponse{CommittedSize: limit}, nil)

	err := MakeCASClient().writeFromClient(bsClientMock, req)
	if err != nil {
		t.Fatalf("Error from client write: %s", err)
	}
}

func TestClientWriteEmpty(t *testing.T) {
	digest := &remoteexecution.Digest{
		Hash:      bazel.EmptySha,
		SizeBytes: bazel.EmptySize,
	}
	err := MakeCASClient().ByteStreamWrite(dialer.NewConstantResolver(""), digest, nil, backoff.NewConstantBackOff(0))
	if err != nil {
		t.Fatal("Expected nil err from empty client write")
	}
}

func TestActionCacheGet(t *testing.T) {
	rc := int32(42)
	req := &remoteexecution.GetActionResultRequest{ActionDigest: &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}}

	mockCtrl := gomock.NewController(t)
	accClientMock := mock_remoteexecution.NewMockActionCacheClient(mockCtrl)

	accClientMock.EXPECT().GetActionResult(context.Background(), req).Return(&remoteexecution.ActionResult{ExitCode: rc}, nil)

	ar, err := MakeCASClient().getCacheFromClient(accClientMock, req)
	if err != nil {
		t.Fatalf("Error from get cache: %s", err)
	}
	if ar.GetExitCode() != rc {
		t.Fatalf("Unexpected result, got %d, want %d", ar.GetExitCode(), rc)
	}
}

func TestActionCacheGetMissing(t *testing.T) {
	req := &remoteexecution.GetActionResultRequest{ActionDigest: &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}}

	mockCtrl := gomock.NewController(t)
	accClientMock := mock_remoteexecution.NewMockActionCacheClient(mockCtrl)

	accClientMock.EXPECT().GetActionResult(context.Background(), req).Return(nil, status.Error(codes.NotFound, ""))

	ar, err := MakeCASClient().getCacheFromClient(accClientMock, req)
	if err == nil {
		t.Fatal("Unexpected non-nil error from GetActionResult")
	}
	if ar != nil {
		t.Fatal("Unexpected non-nil data from GetActionResult")
	}
	if !IsNotFoundError(err) {
		t.Fatalf("Expected NotFoundError, got: %v", err)
	}
}

func TestActionCacheUpdate(t *testing.T) {
	rc := int32(42)
	ar := &remoteexecution.ActionResult{ExitCode: rc}
	ad := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	req := &remoteexecution.UpdateActionResultRequest{ActionDigest: ad, ActionResult: ar}

	mockCtrl := gomock.NewController(t)
	accClientMock := mock_remoteexecution.NewMockActionCacheClient(mockCtrl)

	accClientMock.EXPECT().UpdateActionResult(context.Background(), req).Return(&remoteexecution.ActionResult{ExitCode: rc}, nil)

	arRes, err := MakeCASClient().updateCacheFromClient(accClientMock, req)
	if err != nil {
		t.Fatalf("Error from get cache: %s", err)
	}

	if arRes.GetExitCode() != rc {
		t.Fatalf("Unexpected result, got %d, want %d", arRes.GetExitCode(), rc)
	}
}

func TestBatchRead(t *testing.T) {
	// setup the mock objects for the test
	mockCtrl := gomock.NewController(t)
	mockConn := mock_connection_api.NewMockClientConnPtr(mockCtrl)
	mockConn.EXPECT().Close().Return(nil)
	grpcMock := mock_connection_api.NewMockGRPCDialer(mockCtrl)
	grpcMock.EXPECT().Dial("", gomock.Any()).Return(mockConn, nil)

	var caspbClientMock remoteexecution.ContentAddressableStorageClient
	caspbClientMock = mock_remoteexecution.NewMockContentAddressableStorageClient(mockCtrl)
	caspbClienMaker := func(cc connection_api.ClientConnPtr)remoteexecution.ContentAddressableStorageClient {
		return caspbClientMock}

	// define read request and fake return values
	mockResults := &remoteexecution.BatchReadBlobsResponse{
		Responses:            make([] *remoteexecution.BatchReadBlobsResponse_Response, 10),
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}
	caspbClientMock.(*mock_remoteexecution.MockContentAddressableStorageClient).EXPECT().BatchReadBlobs(gomock.Any(), gomock.Any()).Return(mockResults, nil)

	requestedDownloads := make([]*remoteexecution.Digest, 10)  // list of digests for requested blobs

	// request 10 sha's
	for i:=0; i < 10; i++ {
		d := &remoteexecution.Digest{
			Hash:                 fmt.Sprintf("fakeSha%d", i),
			SizeBytes:            int64(i),
		}
		requestedDownloads[i] = d

		// make the blob that the mock will return
		data := makeRandomData(i+1)
		mockBlob := &remoteexecution.BatchReadBlobsResponse_Response{
			Digest: d,
			Data:   data,
			Status: &rpc_status.Status{Code: int32(rpc_code.Code_OK),},
		}
		mockResults.Responses[i] = mockBlob
	}

	// create the CAS client injecting the mocks
	CASClient := MakeCASClient().
		SetGrpcDialer(grpcMock).
		SetCASpbMaker(caspbClienMaker)

	// issue the Read Request
	downloadedData, e := CASClient.BatchRead(dialer.NewConstantResolver(""), requestedDownloads, backoff.NewConstantBackOff(time.Nanosecond))

	// validate results
	assert.Equal(t, nil, e)

	assert.True(t, 10 == len(downloadedData))  // we should get 10 entries in the map

	for i := 0; i < 10; i++ {
		assert.Equal(t,  i+1, len(downloadedData[requestedDownloads[i].Hash]))
	}

}

func TestBatchWrite(t *testing.T) {
	// setup the test
	mockCtrl := gomock.NewController(t)
	mockConn := mock_connection_api.NewMockClientConnPtr(mockCtrl)
	mockConn.EXPECT().Close().Return(nil)
	grpcMock := mock_connection_api.NewMockGRPCDialer(mockCtrl)
	grpcMock.EXPECT().Dial("", gomock.Any()).Return(mockConn, nil)

	var caspbClientMock remoteexecution.ContentAddressableStorageClient
	caspbClientMock = mock_remoteexecution.NewMockContentAddressableStorageClient(mockCtrl)
	caspbClienMaker := func(cc connection_api.ClientConnPtr)remoteexecution.ContentAddressableStorageClient {
		return caspbClientMock}

	// Make a BatchWriteRequest, and fake return values
	uploadContents := make([]BatchUploadContent, 10)

	// make the structure that the mock proto client will return
	mockResp := &remoteexecution.BatchUpdateBlobsResponse{
		Responses:            make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 10),
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	for i:=0; i < 10; i++ {
		// create upload digest and data
		theData := makeRandomData(i+1)
		sha := sha256.Sum256(theData)
		shaStr := fmt.Sprintf("%x", sha)
		newDigest := &remoteexecution.Digest{
			Hash:                 shaStr,
			SizeBytes:            int64(len(theData)),
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		uploadContent := BatchUploadContent{
			Digest: newDigest,
			Data:   theData,
		}
		uploadContents[i] = uploadContent

		// fill in the mock response
		mockResp.Responses[i] = &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest:               newDigest,
			Status:               &rpc_status.Status{Code: int32(rpc_code.Code_OK)},
		}
	}
	caspbClientMock.(*mock_remoteexecution.MockContentAddressableStorageClient).EXPECT().BatchUpdateBlobs(gomock.Any(), gomock.Any()).Return(mockResp, nil)

	// create the CAS client injecting the mocks
	CASClient := MakeCASClient().
		SetGrpcDialer(grpcMock).
		SetCASpbMaker(caspbClienMaker)

	// issue the batch write request
	digests, e := CASClient.BatchUpdateWrite(dialer.NewConstantResolver(""), uploadContents, backoff.NewConstantBackOff(time.Nanosecond))

	// validate the results
	assert.Equal(t, nil, e)
	assert.Equal(t, 10, len(digests))
}

// create a random Data set
func makeRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}
