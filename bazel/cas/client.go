package cas

import (
	"fmt"

	uuid "github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/dialer"
)

// CAS Client APIs
// These are more straightforward than the server APIs - bytestream provides
// the majority of the CAS Client implementation. We provide wrappers as
// higher-level operations.

// Read data as bytes from a CAS. Takes a Resolver for addressing and a bazel Digest to read.
func ByteStreamRead(r dialer.Resolver, digest *remoteexecution.Digest) ([]byte, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	rname, err := GetDefaultReadResourceName(digest.GetHash(), digest.GetSizeBytes())
	if err != nil {
		return nil, err
	}
	req := &bytestream.ReadRequest{
		ResourceName: rname,
		ReadOffset:   0,
		ReadLimit:    digest.GetSizeBytes(),
	}

	bsc := bytestream.NewByteStreamClient(cc)
	return readFromClient(bsc, req)
}

func readFromClient(bsc bytestream.ByteStreamClient, req *bytestream.ReadRequest) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("Unexpected nil ReadRequest in cas client write")
	}
	if req.ReadOffset != 0 || req.ReadLimit <= 0 {
		return nil, fmt.Errorf("Unsupported ReadRequest - Offset must be 0, Limit must be known > 0")
	}

	rc, err := bsc.Read(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to make Read request: %s", err)
	}

	// Recv from server until Limit reached
	var data []byte
	for bytesRead := int64(0); bytesRead < req.ReadLimit; {
		res, err := rc.Recv()
		if err != nil {
			return nil, fmt.Errorf("Failed Recv'ing data from server: %s", err)
		}
		read := res.GetData()
		if read == nil {
			return nil, fmt.Errorf("Unexpected nil data from ReadResponse")
		}
		data = append(data, read...)
		bytesRead = bytesRead + int64(len(read))
	}
	return data, nil
}

// Write data as bytes to a CAS. Takes a Resolver for addressing, a bazel Digest to read, and []byte data.
func ByteStreamWrite(r dialer.Resolver, digest *remoteexecution.Digest, data []byte) error {
	serverAddr, err := r.Resolve()
	if err != nil {
		return fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	uid, _ := uuid.NewV4()
	wname, err := GetDefaultWriteResourceName(uid.String(), digest.GetHash(), digest.GetSizeBytes())
	if err != nil {
		return err
	}
	req := &bytestream.WriteRequest{
		ResourceName: wname,
		WriteOffset:  0,
		FinishWrite:  true,
		Data:         data,
	}

	bsc := bytestream.NewByteStreamClient(cc)
	return writeFromClient(bsc, req)
}

func writeFromClient(bsc bytestream.ByteStreamClient, req *bytestream.WriteRequest) error {
	if req == nil {
		return fmt.Errorf("Unexpected nil WriteRequest in cas client write")
	}

	wc, err := bsc.Write(context.Background())
	if err != nil {
		return fmt.Errorf("Failed to make Write request: %s", err)
	}

	err = wc.Send(req)
	if err != nil {
		return fmt.Errorf("Failed to send data for write: %s", err)
	}

	res, err := wc.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("Error closing and recv'ing write: %s", err)
	}

	if res.GetCommittedSize() != int64(len(req.GetData())) {
		return fmt.Errorf("Committed size %d did not match data len %d", res.GetCommittedSize(), len(req.GetData()))
	}

	return nil
}
