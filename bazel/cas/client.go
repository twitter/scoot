package cas

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	uuid "github.com/nu7hatch/gouuid"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/dialer"
)

// CAS Client APIs
// These are more straightforward than the server APIs - bytestream provides
// the majority of the CAS Client implementation. We provide wrappers as
// higher-level operations.

// Type that indicates a CAS client operation found because the server returned a GRPC
// NOT_FOUND error.
type NotFoundError struct {
	Err string
}

// Implements Error interface
func (e *NotFoundError) Error() string {
	if e == nil {
		return ""
	}
	return e.Err
}

// Returns true if an error is of type NotFoundError
func IsNotFoundError(err error) bool {
	if _, ok := err.(*NotFoundError); ok {
		return true
	}
	return false
}

// Read data as bytes from a CAS. Takes a Resolver for addressing and a bazel Digest to read.
// Returns bytes read or an error. If the requested resource was not found,
// returns a NotFoundError
// If retries > 0, does simple retry attempts when encountering errors
func ByteStreamRead(r dialer.Resolver, digest *remoteexecution.Digest, retries int) (bytes []byte, err error) {
	// skip request processing for empty sha
	if digest == nil || bazel.IsEmptyDigest(digest) {
		return nil, nil
	}
	if retries < 0 {
		retries = 0
	}

	b := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(retries))
	for ; retries >= 0; retries-- {
		bytes, err = byteStreamRead(r, digest)

		if err == nil || IsNotFoundError(err) {
			break
		}
		time.Sleep(b.NextBackOff())
	}
	return bytes, err
}

func byteStreamRead(r dialer.Resolver, digest *remoteexecution.Digest) ([]byte, error) {
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
	rc, err := bsc.Read(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to get ReadClient: %s", err)
	}

	// Recv from server until Limit reached
	var data []byte
	for bytesRead := int64(0); bytesRead < req.ReadLimit; {
		res, err := rc.Recv()
		if err != nil {
			// If error is a grpc Status, check if it has a grpc NOT_FOUND code
			if grpcStatus, ok := status.FromError(err); ok {
				if grpcStatus.Code() == codes.NotFound {
					return nil, &NotFoundError{Err: grpcStatus.Message()}
				}
			}
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
// If retries > 0, does simple retry attempts when encountering errors
func ByteStreamWrite(r dialer.Resolver, digest *remoteexecution.Digest, data []byte, retries int) (err error) {
	// skip request processing for empty sha
	if digest == nil || bazel.IsEmptyDigest(digest) {
		return nil
	}
	if retries < 0 {
		retries = 0
	}
	b := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(retries))

	for ; retries >= 0; retries-- {
		err = byteStreamWrite(r, digest, data)

		if err == nil {
			break
		}
		time.Sleep(b.NextBackOff())
	}
	return err
}

func byteStreamWrite(r dialer.Resolver, digest *remoteexecution.Digest, data []byte) error {
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

// Client function for GetActionResult requests. Takes a Resolver for ActionCache server and Digest to get.
// If retries > 0, does simple retry attempts when encountering errors
func GetCacheResult(r dialer.Resolver, digest *remoteexecution.Digest, retries int) (ar *remoteexecution.ActionResult, err error) {
	if retries < 0 {
		retries = 0
	}
	for ; retries >= 0; retries-- {
		ar, err = getCacheResult(r, digest)

		if err == nil || IsNotFoundError(err) {
			break
		}
	}
	return ar, err
}

func getCacheResult(r dialer.Resolver, digest *remoteexecution.Digest) (*remoteexecution.ActionResult, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &remoteexecution.GetActionResultRequest{ActionDigest: digest}

	acc := remoteexecution.NewActionCacheClient(cc)
	return getCacheFromClient(acc, req)
}

func getCacheFromClient(acc remoteexecution.ActionCacheClient,
	req *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	ar, err := acc.GetActionResult(context.Background(), req)
	if err != nil {
		// If error is a grpc Status, check if it has a grpc NOT_FOUND code
		if grpcStatus, ok := status.FromError(err); ok {
			if grpcStatus.Code() == codes.NotFound {
				return nil, &NotFoundError{Err: grpcStatus.Message()}
			}
		}
		return nil, fmt.Errorf("Failed to make GetActionResult request: %s", err)
	}

	return ar, nil
}

// Client function for UpdateActionResult requests. Takes a Resolver for ActionCache server and Digest/ActionResult to update.
// If retries > 0, does simple retry attempts when encountering errors
func UpdateCacheResult(r dialer.Resolver, digest *remoteexecution.Digest,
	ar *remoteexecution.ActionResult, retries int) (out *remoteexecution.ActionResult, err error) {
	if retries < 0 {
		retries = 0
	}
	for ; retries >= 0; retries-- {
		out, err = updateCacheResult(r, digest, ar)

		if err == nil {
			break
		}
	}
	return out, err
}

func updateCacheResult(r dialer.Resolver,
	digest *remoteexecution.Digest, ar *remoteexecution.ActionResult) (*remoteexecution.ActionResult, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &remoteexecution.UpdateActionResultRequest{ActionDigest: digest, ActionResult: ar}

	acc := remoteexecution.NewActionCacheClient(cc)
	return updateCacheFromClient(acc, req)
}

func updateCacheFromClient(acc remoteexecution.ActionCacheClient,
	req *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	ar, err := acc.UpdateActionResult(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to make UpdateActionResult request: %s", err)
	}

	return ar, nil
}
