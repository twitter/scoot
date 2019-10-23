package cas

import (
	"fmt"

	"github.com/cenkalti/backoff"
	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	conn "github.com/twitter/scoot/bazel/cas/connection"
	"github.com/twitter/scoot/bazel/remoteexecution"
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

func MakeCASpbClient(cc conn.ClientConnPtr) remoteexecution.ContentAddressableStorageClient {
	return remoteexecution.NewContentAddressableStorageClient(cc.(*grpc.ClientConn))
}

// CASClient struct with exposed 3rd party dependencies (the grpcDialer and function to create protobuf CAS client)
// to allow injecting mocks during testing
type CASClient struct {
	grpcDialer conn.GRPCDialer
	CASpbMaker func(cc conn.ClientConnPtr) remoteexecution.ContentAddressableStorageClient // func that returns the proto client
}

// make a CASClient that uses the production 3rd party libraries.  Testing will overwrite these values
// (using the setters below) with mocks
func MakeCASClient() *CASClient {
	return &CASClient{
		grpcDialer: conn.MakeRealGRPCDialer(), // by default CASClient uses a real grpc dialer
		CASpbMaker: MakeCASpbClient,           // by default CASClient uses a real proto CAS Client
	}
}

// setters use builder pattern (y.Set(x) returns y) for more succinct construction
func (casCli *CASClient) SetGrpcDialer(dialer conn.GRPCDialer) *CASClient {
	casCli.grpcDialer = dialer
	return casCli
}
func (casCli *CASClient) SetCASpbMaker(pcClientMaker func(cc conn.ClientConnPtr) remoteexecution.ContentAddressableStorageClient) *CASClient {
	casCli.CASpbMaker = pcClientMaker
	return casCli
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
func (casCli *CASClient) ByteStreamRead(r dialer.Resolver, digest *remoteexecution.Digest, b backoff.BackOff) (bytes []byte, err error) {
	// skip request processing for empty sha
	if digest == nil || bazel.IsEmptyDigest(digest) {
		return nil, nil
	}
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		bytes, err = casCli.byteStreamRead(r, digest)
		try += 1
		if IsNotFoundError(err) {
			return nil
		}
		return err
	}, b)
	return bytes, err
}

func (casCli *CASClient) byteStreamRead(r dialer.Resolver, digest *remoteexecution.Digest) ([]byte, error) {
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
	return casCli.readFromClient(bsc, req)
}

func (casCli *CASClient) readFromClient(bsc bytestream.ByteStreamClient, req *bytestream.ReadRequest) ([]byte, error) {
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
func (casCli *CASClient) ByteStreamWrite(r dialer.Resolver, digest *remoteexecution.Digest, data []byte, b backoff.BackOff) (err error) {
	// skip request processing for empty sha
	if digest == nil || bazel.IsEmptyDigest(digest) {
		return nil
	}
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		err = casCli.byteStreamWrite(r, digest, data)
		try += 1
		return err
	}, b)
	return err
}

func (casCli *CASClient) byteStreamWrite(r dialer.Resolver, digest *remoteexecution.Digest, data []byte) error {
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
	return casCli.writeFromClient(bsc, req)
}

func (casCli *CASClient) writeFromClient(bsc bytestream.ByteStreamClient, req *bytestream.WriteRequest) error {
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
func (casCli *CASClient) GetCacheResult(r dialer.Resolver,
	digest *remoteexecution.Digest, b backoff.BackOff) (ar *remoteexecution.ActionResult, err error) {
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		ar, err = casCli.getCacheResult(r, digest)
		try += 1
		if IsNotFoundError(err) {
			return nil
		}
		return err
	}, b)
	return ar, err
}

func (casCli *CASClient) getCacheResult(r dialer.Resolver, digest *remoteexecution.Digest) (*remoteexecution.ActionResult, error) {
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
	return casCli.getCacheFromClient(acc, req)
}

func (casCli *CASClient) getCacheFromClient(acc remoteexecution.ActionCacheClient,
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
func (casCli *CASClient) UpdateCacheResult(r dialer.Resolver, digest *remoteexecution.Digest,
	ar *remoteexecution.ActionResult, b backoff.BackOff) (out *remoteexecution.ActionResult, err error) {
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		out, err = casCli.updateCacheResult(r, digest, ar)
		try += 1
		return err
	}, b)
	return out, err
}

func (casCli *CASClient) updateCacheResult(r dialer.Resolver,
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
	return casCli.updateCacheFromClient(acc, req)
}

func (casCli *CASClient) updateCacheFromClient(acc remoteexecution.ActionCacheClient,
	req *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	ar, err := acc.UpdateActionResult(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to make UpdateActionResult request: %s", err)
	}

	return ar, nil
}

type BatchUploadContent struct {
	Digest *remoteexecution.Digest
	Data   []byte
}

/*
client for uploading a list of digests to CAS BatchUpdateBlobs (as a single batch)
*/
func (casCli *CASClient) BatchUpdateWrite(r dialer.Resolver,
	contents []BatchUploadContent, b backoff.BackOff) ([]*remoteexecution.Digest, error) {
	// make the request object
	requests := make([]*remoteexecution.BatchUpdateBlobsRequest_Request, 0)
	totalSize := int64(0)
	for _, content := range contents {
		requests = append(requests, &remoteexecution.BatchUpdateBlobsRequest_Request{
			Digest: content.Digest,
			Data:   content.Data,
		})
		log.Infof("uploading: %v", content.Digest)
		totalSize += content.Digest.SizeBytes
	}

	if totalSize > bazel.BatchMaxCombinedSize {
		return nil, fmt.Errorf("batch size too big. %d > the max %d", totalSize, bazel.BatchMaxCombinedSize)
	}

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: bazel.DefaultInstanceName,
		Requests:     requests,
	}

	var ctx context.Context = context.Background()

	// upload with retries
	try := 1
	var successfulUploads []*remoteexecution.Digest
	e := backoff.Retry(func() error {
		var err error
		log.Debugf("Try #%d", try)
		successfulUploads, err = casCli.batchUpdateWriter(r, request, ctx)
		if err != nil {
			log.Infof("try:#%d upload error:%s", try, err.Error())
		}
		try += 1
		return err
	}, b)

	// if errors are found return an error result
	if e != nil {
		return successfulUploads, fmt.Errorf("CAS BatchUpdateBlobs() returned error:%s", e.Error())
	}

	return successfulUploads, nil
}

// (retryable) method for CAS batch update
func (casCli *CASClient) batchUpdateWriter(r dialer.Resolver,
	request *remoteexecution.BatchUpdateBlobsRequest, ctx context.Context) ([]*remoteexecution.Digest, error) {
	// create a CAS object with connection to server
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}
	cc, err := casCli.grpcDialer.Dial(serverAddr, grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(bazel.BatchMaxCombinedSize),
			grpc.MaxCallSendMsgSize(bazel.BatchMaxCombinedSize)))
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	// issue the request
	cas := casCli.CASpbMaker(cc) // create a pb remoteexecution client
	resp, e := cas.BatchUpdateBlobs(ctx, request)

	// parse the response for errors
	if e != nil {
		return nil, e
	}

	errs := make([]string, 0)
	digests := make([]*remoteexecution.Digest, 0)
	for _, r := range resp.Responses {
		if rpc_code.Code(r.Status.Code) == rpc_code.Code_OK {
			digests = append(digests, r.Digest)
		} else {
			errs = append(errs, r.Status.Message)
		}
	}
	if len(errs) > 0 {
		return digests, fmt.Errorf("%v", errs)
	}

	return digests, nil
}

/*
client for downloading a list of digests from the CAS BatchReadBlobs (as a single batch)
*/
func (casCli *CASClient) BatchRead(r dialer.Resolver,
	digests []*remoteexecution.Digest, b backoff.BackOff) (map[string][]byte, error) {
	// make the request object
	request := &remoteexecution.BatchReadBlobsRequest{
		InstanceName: bazel.DefaultInstanceName,
		Digests:      digests,
	}

	var ctx context.Context = context.Background()
	var content map[string][]byte
	var err error

	// download with retries
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		content, err = casCli.batchReader(r, request, ctx)
		try += 1
		return err
	}, b)

	return content, err
}

// (retryable) method for CAS batch update
func (casCli *CASClient) batchReader(r dialer.Resolver,
	request *remoteexecution.BatchReadBlobsRequest, ctx context.Context) (map[string][]byte, error) {
	// create a CAS object with connection to server
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}
	cc, err := casCli.grpcDialer.Dial(serverAddr, grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(bazel.BatchMaxCombinedSize),
			grpc.MaxCallSendMsgSize(bazel.BatchMaxCombinedSize)))
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	// issue the request
	cas := casCli.CASpbMaker(cc) // create a pb remoteexecution client
	resp, e := cas.BatchReadBlobs(ctx, request)

	if e != nil {
		return nil, e
	}

	// extract content and errors from the response
	errs := make([]string, 0)
	content := make(map[string][]byte)
	for _, r := range resp.Responses {
		if rpc_code.Code(r.Status.Code) != rpc_code.Code_OK {
			errs = append(errs, r.Status.Message)
		} else {
			content[r.Digest.Hash] = r.Data
		}
	}

	if len(errs) == 0 {
		return content, nil
	}
	return content, fmt.Errorf("%v", errs)
}

// Make a FindMissingBlobs request to a CAS given a set of digests. Returns digests that are missing on the server.
func (casCli *CASClient) FindMissingBlobs(r dialer.Resolver,
	digests []*remoteexecution.Digest, b backoff.BackOff) (missing []*remoteexecution.Digest, err error) {
	res := &remoteexecution.FindMissingBlobsResponse{}
	try := 1
	backoff.Retry(func() error {
		log.Debugf("Try #%d", try)
		res, err = casCli.findMissingBlobs(r, digests)
		try += 1
		return err
	}, b)
	for _, d := range res.GetMissingBlobDigests() {
		missing = append(missing, d)
	}
	return missing, nil
}

func (casCli *CASClient) findMissingBlobs(r dialer.Resolver,
	digests []*remoteexecution.Digest) (*remoteexecution.FindMissingBlobsResponse, error) {
	serverAddr, err := r.Resolve()
	if err != nil {
		return nil, fmt.Errorf("Failed to resolve server address: %s", err)
	}

	cc, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to dial server %s: %s", serverAddr, err)
	}
	defer cc.Close()

	req := &remoteexecution.FindMissingBlobsRequest{BlobDigests: digests}

	casc := remoteexecution.NewContentAddressableStorageClient(cc)
	return casCli.findMissingBlobsFromClient(casc, req)
}

func (casCli *CASClient) findMissingBlobsFromClient(casc remoteexecution.ContentAddressableStorageClient,
	req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	res, err := casc.FindMissingBlobs(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("Failed to make FindMissingBlobs request: %s", err)
	}
	return res, err
}
