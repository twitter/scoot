package cas

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
)

// CAS Client APIs
// These are more straightforward than the server APIs - bytestream provides
// the majority of the CAS Client implementation. We provide wrappers as
// higher-level operations.

// Read data as bytes from a CAS. Takes address in "host:port" format and a bazel Digest to read.
// Scoot only supports CAS API over a network grpc interface (i.e. an apiserver)
func ByteStreamRead(serverAddr string, d *remoteexecution.Digest) ([]byte, error) {
	cc, err := grpc.DialContext(context.Background(), serverAddr)
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	rname, err := GetReadResourceName("", d.GetHash(), d.GetSizeBytes(), "")
	if err != nil {
		return nil, err
	}
	req := &bytestream.ReadRequest{
		ResourceName: rname,
		ReadOffset:   0,
		ReadLimit:    d.GetSizeBytes(),
	}

	bsc := bytestream.NewByteStreamClient(cc)
	return readFromClient(bsc, req)
}

func readFromClient(bsc bytestream.ByteStreamClient, req *bytestream.ReadRequest) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("Unexpected nil ReadRequest in cas ClientRead")
	}
	if req.ReadOffset != 0 || req.ReadLimit <= 0 {
		return nil, fmt.Errorf("Unsupported ReadRequest - Offset must be 0, Limit must be known > 0")
	}

	rc, err := bsc.Read(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to make Read request: %s", err)
	}

	// Recv from seerver until Limit reached
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
