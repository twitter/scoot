package cas

//go:generate mockgen -destination=mock_bytestream/bsclient_mock.go google.golang.org/genproto/googleapis/bytestream ByteStreamClient,ByteStream_ReadClient,ByteStream_WriteClient

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas/mock_bytestream"
	"github.com/twitter/scoot/bazel/execution/mock_remoteexecution"
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

	data, err := readFromClient(bsClientMock, req)
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

	data, err := readFromClient(bsClientMock, req)
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
	data, err := ByteStreamRead(dialer.NewConstantResolver(""), digest, 0)
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

	err := writeFromClient(bsClientMock, req)
	if err != nil {
		t.Fatalf("Error from client write: %s", err)
	}
}

func TestClientWriteEmpty(t *testing.T) {
	digest := &remoteexecution.Digest{
		Hash:      bazel.EmptySha,
		SizeBytes: bazel.EmptySize,
	}
	err := ByteStreamWrite(dialer.NewConstantResolver(""), digest, nil, 0)
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

	ar, err := getCacheFromClient(accClientMock, req)
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

	ar, err := getCacheFromClient(accClientMock, req)
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

	arRes, err := updateCacheFromClient(accClientMock, req)
	if err != nil {
		t.Fatalf("Error from get cache: %s", err)
	}

	if arRes.GetExitCode() != rc {
		t.Fatalf("Unexpected result, got %d, want %d", arRes.GetExitCode(), rc)
	}
}
