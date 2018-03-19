package cas

// To generate required mockgen files for these tests, from the Top Level github.com/twitter/scoot dir:
//
//	(prerequisite: go get google.golang.org/genproto/googleapis/bytestream)
// 	mockgen google.golang.org/genproto/googleapis/bytestream ByteStreamClient,ByteStream_ReadClient,ByteStream_WriteClient > bazel/cas/mock_bytestream/bsclient_mock.go
//	NOTE: in the generated file, replace the "context" import with "golang.org/x/net/context"
//	this seems to be a go version/mock incompatability
//

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	uuid "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"

	"github.com/twitter/scoot/bazel/cas/mock_bytestream"
)

func TestClientRead(t *testing.T) {
	// Make a ReadRequest with a known limit
	offset, limit := int64(0), testSize1
	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/%d", testHash1, limit), ReadOffset: offset, ReadLimit: limit}

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
