package cas

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas/mock_bytestream"
	"github.com/twitter/scoot/snapshot/store"
)

func TestClientRead(t *testing.T) {
	f := &store.FakeStore{}

	// Write a resource to underlying store
	d := &remoteexecution.Digest{Hash: testHash1, SizeBytes: testSize1}
	resourceName := bazel.DigestStoreName(d)
	err := f.Write(resourceName, bytes.NewReader(testData1), nil)
	if err != nil {
		t.Fatalf("Failed to write into FakeStore: %v", err)
	}

	// Make a ReadRequest with a known limit
	offset, limit := int64(0), testSize1
	req := &bytestream.ReadRequest{ResourceName: fmt.Sprintf("blobs/%s/-1", testHash1), ReadOffset: offset, ReadLimit: limit}

	mockCtrl := gomock.NewController(t)
	bsClientMock := mock_bytestream.NewMockByteStreamClient(mockCtrl)
	bsReadClientMock := mock_bytestream.NewMockByteStream_ReadClient(mockCtrl)

	bsClientMock.EXPECT().Read(context.Background(), req).Return(bsReadClientMock, nil)
	bsReadClientMock.EXPECT().Recv().Return(&bytestream.ReadResponse{Data: testData1}, nil)

	data, err := readFromClient(bsClientMock, req)
	if err != nil {
		t.Fatalf("Error from ClientRead: %s", err)
	}

	if bytes.Compare(testData1, data) != 0 {
		t.Fatalf("Data read from client did not match - expected: %s, got: %s", testData1, data)
	}
}
