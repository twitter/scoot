// Bazel Remote Execution API gRPC server
// Contains limited implementation of the ContentAddressableStore API interface
package cas

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/grpchelpers"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/snapshot/store"
)

// Implements GRPCServer, remoteexecution.ContentAddressableStoreServer,
// remoteexecution.ActionCacheServer, bytestream.ByteStreamServer interfaces
type casServer struct {
	listener    net.Listener
	server      *grpc.Server
	storeConfig *store.StoreConfig
	stat        stats.StatsReceiver
	concurrent  chan struct{} // TODO candidate for removal
}

// Creates a new GRPCServer (CASServer/ByteStreamServer/ActionCacheServer)
// based on a listener, and preregisters the service
// TODO closed src apiserver main.go is set up with a LimitListener at 100
func MakeCASServer(l net.Listener, cfg *store.StoreConfig, stat stats.StatsReceiver) *casServer {
	// TODO experimental
	maxStreamOpt := grpc.MaxConcurrentStreams(10)
	tapLimiterOpt := grpc.InTapHandle(newTap().Handler)
	g := casServer{
		listener:    l,
		server:      grpchelpers.NewServer(maxStreamOpt, tapLimiterOpt),
		storeConfig: cfg,
		stat:        stat,
		//concurrent:  make(chan struct{}, MaxConnections), TODO left as nil to disable blocking
	}
	remoteexecution.RegisterContentAddressableStorageServer(g.server, &g)
	remoteexecution.RegisterActionCacheServer(g.server, &g)
	bytestream.RegisterByteStreamServer(g.server, &g)
	return &g
}

type tapLimiter struct {
	limiter *rate.Limiter
}

// TODO experimental
func newTap() *tapLimiter {
	return &tapLimiter{limiter: rate.NewLimiter(100, 20)}
}

func (t *tapLimiter) Handler(ctx context.Context, info *tap.Info) (context.Context, error) {
	// if no deadline is set in context, add one so this request can time out
	if _, ok := ctx.Deadline(); !ok {
		ctx, _ = context.WithDeadline(ctx, time.Now().Add(10*time.Second))
	}
	err := t.limiter.Wait(ctx)
	if err != nil {
		log.Error("Tap dropped connection waiting for rate limiter")
		return nil, status.Errorf(codes.ResourceExhausted, "Service exhausted while waiting for rate limiter")
	}
	return ctx, nil
}

func (s *casServer) IsInitialized() bool {
	if s == nil {
		return false
	} else if s.storeConfig == nil {
		return false
	}
	return true
}

func (s *casServer) Serve() error {
	log.Info("Serving GRPC CAS API on: ", s.listener.Addr())
	return s.server.Serve(s.listener)
}

// CAS APIs

// FindMissingBlobs returns a list of digests of blobs that are not available on the server
func (s *casServer) FindMissingBlobs(
	ctx context.Context,
	req *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	log.Debugf("Received CAS FindMissingBlobs request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}
	if s.concurrent != nil {
		s.concurrent <- struct{}{}
		defer func() { <-s.concurrent }()
	}

	var err error = nil
	var length int64 = 0

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzFindBlobsSuccessCounter).Inc(1)
			s.stat.Histogram(stats.BzFindBlobsLengthHistogram).Update(length)
		} else {
			s.stat.Counter(stats.BzFindBlobsFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzFindBlobsLatency_ms).Time().Stop()

	res := remoteexecution.FindMissingBlobsResponse{}
	length = int64(len(req.GetBlobDigests()))

	for _, digest := range req.GetBlobDigests() {
		// We hardcode support for empty data in snapshot/filer/checkouter.go, so never report it as missing
		// Empty SHA can be used to represent working with a plain, empty directory, but can cause problems in Stores
		if digest.GetHash() == bazel.EmptySha {
			continue
		}

		storeName := bazel.DigestStoreName(digest)
		if exists, err := s.storeConfig.Store.Exists(storeName); err != nil {
			log.Errorf("Error checking existence: %v", err)
			return nil, status.Error(codes.Internal, fmt.Sprintf("Store failed checking existence of %s: %v", storeName, err))
		} else if !exists {
			res.MissingBlobDigests = append(res.MissingBlobDigests, digest)
		}
	}

	log.Infof("Returning missing blob digests: %s", res.MissingBlobDigests)
	return &res, nil
}

// BatchUpdate not supported in Scoot for V1
func (s *casServer) BatchUpdateBlobs(
	ctx context.Context,
	req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot - update blobs independently")
}

// GetTree not supported in Scoot for V1
func (s *casServer) GetTree(
	req *remoteexecution.GetTreeRequest, gtServer remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "Currently unsupported in Scoot")
}

// ByteStream APIs

// Serves content in the bundlestore to a client via grpc streaming.
// Implements googleapis bytestream Read
// TODO one log per request...
func (s *casServer) Read(req *bytestream.ReadRequest, ser bytestream.ByteStream_ReadServer) error {
	log.Debugf("Received CAS Read request: %s", req)

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}
	if s.concurrent != nil {
		s.concurrent <- struct{}{}
		defer func() { <-s.concurrent }()
	}

	var length int64 = 0
	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzReadSuccessCounter).Inc(1)
			if length > 0 {
				s.stat.Histogram(stats.BzReadBytesHistogram).Update(length)
			}
		} else {
			s.stat.Counter(stats.BzReadFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzReadLatency_ms).Time().Stop()

	// Parse resource name per Bazel API specification
	resource, err := ParseReadResource(req.GetResourceName())
	if err != nil {
		log.Errorf("Failed to parse resource name: %v", err)
		return status.Error(codes.InvalidArgument, fmt.Sprintf("%v", err))
	}

	// Input validation per API spec
	if req.GetReadOffset() < 0 {
		log.Error("Invalid read offset")
		return status.Error(codes.OutOfRange, fmt.Sprintf("Invalid read offset %d", req.GetReadOffset()))
	}
	if req.GetReadLimit() < 0 {
		log.Error("Invalid read limit")
		return status.Error(codes.InvalidArgument, "Read limit < 0 invalid")
	}

	// Map digest to underlying store name
	storeName := bazel.DigestStoreName(resource.Digest)

	var r io.ReadCloser
	// If client requested to read Empty data, fulfil the request with a blank interface to bypass the Store
	if resource.Digest.GetHash() == bazel.EmptySha {
		r = &nilReader{}
	} else {
		log.Infof("Opening store resource for reading: %s", storeName)
		r, err = s.storeConfig.Store.OpenForRead(storeName)
		if err != nil {
			// If an error occurred opening the underlying resource, we interpret this as NotFound.
			// Although we return an error response to the caller to indicate this, we regard this
			// as a normal defined behavior of the API, and don't count it towards failure metrics.
			// Reset err to nil to prevent recording as a failure.
			openErr := err
			err = nil
			log.Errorf("Failed to OpenForRead: %v", openErr)
			return status.Error(codes.NotFound, fmt.Sprintf("Failed opening resource %s for read, returning NotFound. Err: %v", storeName, openErr))
		}
	}
	defer r.Close()

	res := &bytestream.ReadResponse{}
	c := int64(DefaultReadCapacity)

	// If ReadOffset was specified, discard bytes prior to it
	if req.GetReadOffset() > 0 {
		_, err = io.CopyN(ioutil.Discard, r, req.GetReadOffset())
		if err != nil {
			log.Errorf("Failed reading until offset: %v", err)
			return status.Error(codes.Internal, "Failed to read until offset")
		}
	}
	// Set a capacity based on ReadLimit or content size
	if req.GetReadLimit() > 0 && req.GetReadLimit() < c {
		c = req.GetReadLimit()
	}

	/* TODO the memory culprit
	Is there a way to use a buffer pool here?
	So I limit this function to say 100 concurrent, but then here I am doing N chunks of 1MB
	in a for loop, but still in parallel, if Send() passes off to an async server stream backend?
	* Try: get rid of MaxConns and channel restrictors. Log how many Sends were needed? Call GC() here?
	*/
	// Read data in chunks and stream to client
	p := make([]byte, 0, c)
	for {
		n, err := r.Read(p[:cap(p)])
		p = p[:n]
		length += int64(n)

		if n > 0 {
			res.Data = p
			err = ser.Send(res)
			if err != nil {
				log.Errorf("Failed to Send(): %v", err)
				return status.Error(codes.Internal, fmt.Sprintf("Failed to send ReadResponse: %v", err))
			}
			res.Reset()
		}

		if err == nil {
			continue
		} else if err == io.EOF {
			// We expect to hit an EOF, non-nil error condition as the conclusion of a normal read.
			// Reset err to nil here to prevent recording this as a failure in metrics.
			err = nil
			break
		} else {
			log.Errorf("Failed to read from Store: %v", err)
			return status.Error(codes.Internal, fmt.Sprintf("Failed to read from Store: %v", err))
		}
	}
	log.Infof("Finished sending data for Read from %s with len %d", storeName, length)
	return nil
}

// Writes data into bundlestore from a client via grpc streaming.
// Implements googleapis bytestream Write
// store.Stores do not support partial Writes, and neither does our implementation.
// We can support partial Write by keeping buffers for inflight requests in the casServer.
// When the entire Write is buffered, we can Write to the Store and return a response with the result.
// NOTE We also no not currently attempt any resolution between multiple client UUIDs writing the same resource
func (s *casServer) Write(ser bytestream.ByteStream_WriteServer) error {
	log.Debug("Received CAS Write request")

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}
	if s.concurrent != nil {
		s.concurrent <- struct{}{}
		defer func() { <-s.concurrent }()
	}

	var p []byte
	var buffer *bytes.Buffer
	var committed int64 = 0
	var resource *Resource = nil
	resourceName, storeName := "", ""
	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzWriteSuccessCounter).Inc(1)
			if committed > 0 {
				s.stat.Histogram(stats.BzWriteBytesHistogram).Update(committed)
			}
		} else {
			s.stat.Counter(stats.BzWriteFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzWriteLatency_ms).Time().Stop()

	// As indicated above, not supporting partial/resumable Writes for now.
	// Reads in a stream of data from the client, and proceeds when we've gotten it all.
	for {
		wr, err := ser.Recv()
		if err != nil {
			log.Errorf("Failed to Recv(): %v", err)
			return status.Error(codes.Internal, fmt.Sprintf("Failed to Recv: %v", err))
		}

		// Set up resource on initial WriteRequest. Initialize read buffer based on Digest size
		if resource == nil {
			resourceName = wr.GetResourceName()

			resource, err = ParseWriteResource(resourceName)
			if err != nil {
				log.Errorf("Error parsing resource: %v", err)
				return status.Error(codes.InvalidArgument, fmt.Sprintf("%v", err))
			}
			log.Debugf("Using resource name: %s", resourceName)

			// If the client is attempting to write empty/nil/size-0 data, just return as if we succeeded
			if resource.Digest.GetHash() == bazel.EmptySha {
				log.Infof("Request to write empty sha - bypassing Store write and Closing")
				res := &bytestream.WriteResponse{CommittedSize: bazel.EmptySize}
				err = ser.SendAndClose(res)
				if err != nil {
					log.Errorf("Error during SendAndClose() for EmptySha: %s", err)
					return status.Error(codes.Internal, fmt.Sprintf("Failed to SendAndClose: %v", err))
				}
				return nil
			}

			p = make([]byte, 0, resource.Digest.GetSizeBytes())
			buffer = bytes.NewBuffer(p)

			// If data Exists, terminate immediately with size of existing data (Store is immutable)
			// Note that Store does not support `stat`, so we trust client-provided size to avoid reading the data
			storeName = bazel.DigestStoreName(resource.Digest)
			if exists, err := s.storeConfig.Store.Exists(storeName); err != nil {
				log.Errorf("Error checking existence: %v", err)
				return status.Error(codes.Internal, fmt.Sprintf("Store failed checking existence of %s: %v", storeName, err))
			} else if exists {
				log.Infof("Resource exists in store: %s. Using client digest size: %d", storeName, resource.Digest.GetSizeBytes())
				res := &bytestream.WriteResponse{CommittedSize: resource.Digest.GetSizeBytes()}
				err = ser.SendAndClose(res)
				if err != nil {
					log.Errorf("Error during SendAndClose() for Existing: %v", err)
					return status.Error(codes.Internal, fmt.Sprintf("Failed to SendAndClose WriteResponse: %v", err))
				}
				return nil
			}
		}

		// Validate subsequent WriteRequest fields
		if wr.GetResourceName() != "" && resourceName != wr.GetResourceName() {
			log.Errorf("Invalid resource name in subsequent request: %s", wr.GetResourceName())
			return status.Error(codes.InvalidArgument, fmt.Sprintf("ResourceName %s mismatch with previous %s", wr.GetResourceName(), resourceName))
		}
		if wr.GetWriteOffset() != committed {
			log.Error("Invalid write offset")
			return status.Error(codes.InvalidArgument, fmt.Sprintf("WriteOffset invalid: got %d after committing %d bytes", wr.GetWriteOffset(), committed))
		}

		buffer.Write(wr.GetData())
		committed += int64(len(wr.GetData()))

		// Per API, client indicates all data has been sent
		if wr.GetFinishWrite() {
			break
		}
	}

	// Get committed length and verify - Digest size can be arbitrarily set by the client, but is a trusted value after insertion
	if committed != resource.Digest.GetSizeBytes() {
		log.Errorf("Data length/digest mismatch: %d/%d", committed, resource.Digest.GetSizeBytes())
		return status.Error(codes.Internal, fmt.Sprintf("Data to be written len: %d mismatch with request Digest size: %d", committed, resource.Digest.GetSizeBytes()))
	}

	// Write to underlying Store
	// TODO use CAS Default TTL setting until API supports cache priority settings
	ttl := store.GetTTLValue(s.storeConfig.TTLCfg)
	if ttl != nil {
		ttl.TTL = time.Now().Add(DefaultTTL)
	}

	err = s.storeConfig.Store.Write(storeName, buffer, ttl)
	if err != nil {
		log.Errorf("Store failed to Write: %v", err)
		return status.Error(codes.Internal, fmt.Sprintf("Store failed writing to %s: %v", storeName, err))
	}

	res := &bytestream.WriteResponse{CommittedSize: committed}
	err = ser.SendAndClose(res)
	if err != nil {
		log.Errorf("Error during SendAndClose(): %v", err)
		return status.Error(codes.Internal, fmt.Sprintf("Failed to SendAndClose WriteResponse: %v", err))
	}

	log.Infof("Finished handling Write request for %s, %d bytes", storeName, committed)
	return nil
}

// QueryWriteStatus gives status information about a Write operation in progress
// Unsupported, may be added later for V1
func (s *casServer) QueryWriteStatus(
	context.Context, *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot - Writes are not resumable")
}

// ActionCache APIs

// V2 API requires explicit uploading of Actions prior to Execution. Because of this,
// cached ActionResults cannot be directly addressed by the ActionDigest (collision).
// We address the ActionResult by serializing the ActionDigest with a constant string
// and getting its hash to use for a unique and predictable blobstore address.
type cacheResultAddress struct {
	actionDigest *remoteexecution.Digest
	storeName    string
}

func makeCacheResultAddress(ad *remoteexecution.Digest) (*cacheResultAddress, error) {
	if ad == nil {
		return nil, fmt.Errorf("Nil ActionDigest provided to NewResultAddress")
	}

	// form consistent string using ActionDigest, turn into bytes, transform into store name
	addressKey := fmt.Sprintf("%s-%s", ad.GetHash(), ResultAddressKey)
	buffer := bytes.NewBufferString(addressKey)
	addressBytes := buffer.Bytes()
	len := len(addressBytes)
	sha := fmt.Sprintf("%x", sha256.Sum256(addressBytes))
	addressDigest, err := bazel.DigestFromString(fmt.Sprintf("%s/%d", sha, len))
	if err != nil {
		return nil, fmt.Errorf("Failed to create digest for cache result address: %v", err)
	}

	return &cacheResultAddress{
		actionDigest: ad,
		storeName:    bazel.DigestStoreName(addressDigest),
	}, nil
}

// Retrieve a cached execution result. Results are keyed on ActionDigests of run commands.
func (s *casServer) GetActionResult(ctx context.Context,
	req *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	log.Debugf("Received GetActionResult request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzGetActionSuccessCounter).Inc(1)
		} else {
			s.stat.Counter(stats.BzGetActionFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzGetActionLatency_ms).Time().Stop()

	// Validate input digest
	if !bazel.IsValidDigest(req.GetActionDigest().GetHash(), req.GetActionDigest().GetSizeBytes()) {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Invalid ActionDigest %s", req.GetActionDigest()))
	}

	// If nil digest was requested, that's odd - return nil action result
	if req.GetActionDigest().GetHash() == bazel.EmptySha {
		log.Debug("GetActionResult - returning empty ActionResult from request for EmptySha ActionDigest")
		return &remoteexecution.ActionResult{}, nil
	}

	address, err := makeCacheResultAddress(req.GetActionDigest())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to create cache result address: %v", err))
	}

	// Attempt to read AR from Store. If we error on opening, assume the resource was not found
	log.Infof("Opening store resource for reading: %s", address.storeName)

	r, err := s.storeConfig.Store.OpenForRead(address.storeName)
	if err != nil {
		// If an error occurred opening the underlying resource, we interpret this as NotFound.
		// Although we return an error response to the caller to indicate this, we regard this
		// as a normal defined behavior of the API, and don't count it towards failure metrics.
		// Reset err to nil to prevent recording as a failure.
		openErr := err
		err = nil
		log.Errorf("Failed to OpenForRead: %v", openErr)
		return nil, status.Error(codes.NotFound, fmt.Sprintf("Failed opening %s for read, returning NotFound. Err: %v", address.storeName, openErr))
	}
	defer r.Close()

	arAsBytes, err := ioutil.ReadAll(r)
	if err != nil {
		log.Errorf("Failed reading ActionResult from Store (resource %s): %s", address.storeName, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error reading from %s: %s", address.storeName, err))
	}

	// Deserialize store data as AR
	ar := &remoteexecution.ActionResult{}
	err = proto.Unmarshal(arAsBytes, ar)
	if err != nil {
		log.Errorf("Failed to deserialize bytes from %s as ActionResult: %s", address.storeName, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error deserializing ActionResult: %s", err))
	}

	log.Infof("GetActionResult returning cached result: %s", ar)
	return ar, nil
}

// Client-facing service for caching ActionResults. Support is optional per Bazel API,
// as the server can still cache and retrieve results internally.
func (s *casServer) UpdateActionResult(ctx context.Context,
	req *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	log.Debugf("Received UpdateActionResult request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzUpdateActionSuccessCounter).Inc(1)
		} else {
			s.stat.Counter(stats.BzUpdateActionFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzUpdateActionLatency_ms).Time().Stop()

	// Validate input digest, ActionResult
	if !bazel.IsValidDigest(req.GetActionDigest().GetHash(), req.GetActionDigest().GetSizeBytes()) {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("Invalid ActionDigest %s", req.GetActionDigest()))
	}
	if req.GetActionResult() == nil {
		return nil, status.Error(codes.InvalidArgument, "ActionResult is nil")
	}

	// No-op if user requested to store nil action digest
	if req.GetActionDigest().GetHash() == bazel.EmptySha {
		log.Debug("UpdateActionResult - returning empty ActionResult from request to store with EmptySha digest")
		return &remoteexecution.ActionResult{}, nil
	}

	// serialize the AR as bytes, then Store.Write.
	asBytes, err := proto.Marshal(req.GetActionResult())
	if err != nil {
		log.Errorf("Failed to serialize ActionResult %s as bytes: %s", req.GetActionResult(), err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Error serializing ActionResult: %s", err))
	}

	address, err := makeCacheResultAddress(req.GetActionDigest())
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("Failed to create cache result address: %v", err))
	}

	// Write to store
	// TODO use CAS Default TTL setting until API supports cache priority settings
	ttl := store.GetTTLValue(s.storeConfig.TTLCfg)
	if ttl != nil {
		ttl.TTL = time.Now().Add(DefaultTTL)
	}

	err = s.storeConfig.Store.Write(address.storeName, bytes.NewReader(asBytes), ttl)
	if err != nil {
		log.Errorf("Store failed to Write: %v", err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("Store failed writing to %s: %v", address.storeName, err))
	}

	log.Infof("UpdateActionResult wrote result to cache: %s", req.GetActionResult())
	return req.GetActionResult(), nil
}

// Internal functions

// Interface for reading Empty data in a normal way while bypassing the underlying store
type nilReader struct{}

func (n *nilReader) Read([]byte) (int, error) { return 0, io.EOF }
func (n *nilReader) Close() error             { return nil }
