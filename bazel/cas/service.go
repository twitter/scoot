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
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "github.com/twitter/scoot/bazel/remoteexecution"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	google_rpc_code "google.golang.org/genproto/googleapis/rpc/code"
	google_rpc_status "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
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
}

// Creates a new GRPCServer (CASServer/ByteStreamServer/ActionCacheServer)
// based on GRPCConfig, StoreConfig, and StatsReceiver, and preregisters the service
func MakeCASServer(gc *bazel.GRPCConfig, sc *store.StoreConfig, stat stats.StatsReceiver) *casServer {
	if gc == nil {
		return nil
	}

	l, err := gc.NewListener()
	if err != nil {
		panic(err)
	}
	gs := gc.NewGRPCServer()
	g := casServer{
		listener:    l,
		server:      gs,
		storeConfig: sc,
		stat:        stat,
	}
	remoteexecution.RegisterContentAddressableStorageServer(g.server, &g)
	remoteexecution.RegisterActionCacheServer(g.server, &g)
	bytestream.RegisterByteStreamServer(g.server, &g)
	return &g
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

	sem := make(chan struct{}, BatchParallelism)
	resultCh := make(chan *remoteexecution.Digest)
	var wg sync.WaitGroup
	res := remoteexecution.FindMissingBlobsResponse{}
	length = int64(len(req.GetBlobDigests()))
	log.Infof("Processing CAS FindMissingBlobs request of length: %d", length)

	go func() {
		for d := range resultCh {
			if d != nil {
				res.MissingBlobDigests = append(res.MissingBlobDigests, d)
			}
			<-sem
			wg.Done()
		}
	}()

	// Perform operations in goroutines
	for _, digest := range req.GetBlobDigests() {
		if err != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(d *remoteexecution.Digest) {
			// We hardcode support for empty data in snapshot/filer/checkouter.go, so never report it as missing
			// Empty SHA can be used to represent working with a plain, empty directory, but can cause problems in Stores
			if d.GetHash() == bazel.EmptySha {
				resultCh <- nil
				return
			}

			storeName := bazel.DigestStoreName(d)
			if exists, err := s.storeConfig.Store.Exists(storeName); err != nil {
				log.Errorf("Error checking existence of %s: %v", storeName, err)
				err = fmt.Errorf("Store failed checking existence of one or more digests")
			} else if !exists {
				resultCh <- d
				return
			}
			resultCh <- nil
		}(digest)
	}

	wg.Wait()
	close(resultCh)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Infof("Returning CAS FindMissingBlobs missing digests: %s", res.MissingBlobDigests)
	return &res, nil
}

// BatchUpdateBlobs writes a batch of inlined blob data into bundlestore with parallelism
func (s *casServer) BatchUpdateBlobs(
	ctx context.Context,
	req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	log.Debugf("Received CAS BatchUpdateBlobs request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	var length int64 = 0
	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzBatchUpdateSuccessCounter).Inc(1)
			if length > 0 {
				s.stat.Histogram(stats.BzBatchUpdateLengthHistogram).Update(length)
			}
		} else {
			s.stat.Counter(stats.BzBatchUpdateFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzBatchUpdateLatency_ms).Time().Stop()

	sem := make(chan struct{}, BatchParallelism)
	resultCh := make(chan *remoteexecution.BatchUpdateBlobsResponse_Response)
	var wg sync.WaitGroup
	res := &remoteexecution.BatchUpdateBlobsResponse{}
	length = int64(len(req.GetRequests()))
	log.Infof("Processing CAS BatchUpdateBlobs request of length: %d", length)

	go func() {
		for r := range resultCh {
			res.Responses = append(res.GetResponses(), r)
			<-sem
			wg.Done()
		}
	}()

	// Perform operations in goroutines
	for _, blobReq := range req.GetRequests() {
		if err != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(r *remoteexecution.BatchUpdateBlobsRequest_Request) {
			writeRes := &remoteexecution.BatchUpdateBlobsResponse_Response{Digest: r.GetDigest()}

			// Return OK if we got a request to write the empty sha
			if r.GetDigest().GetHash() == bazel.EmptySha {
				writeRes.Status = &google_rpc_status.Status{
					Code: int32(google_rpc_code.Code_OK),
				}
				resultCh <- writeRes
				return
			}

			// Verify data length with Digest size
			if int64(len(r.GetData())) != r.GetDigest().GetSizeBytes() {
				log.Errorf("Data length/digest mismatch: %d/%d", len(r.GetData()), r.GetDigest().GetSizeBytes())
				writeRes.Status = &google_rpc_status.Status{
					Code:    int32(google_rpc_code.Code_INVALID_ARGUMENT),
					Message: fmt.Sprintf("Data to be written len: %d mismatch with request Digest size: %d", len(r.GetData()), r.GetDigest().GetSizeBytes()),
				}
				resultCh <- writeRes
				return
			}
			// Verify buffer SHA with Digest SHA
			if bufferHash := fmt.Sprintf("%x", sha256.Sum256(r.GetData())); bufferHash != r.GetDigest().GetHash() {
				log.Errorf("Data hash/digest hash mismatch: %s/%s", bufferHash, r.GetDigest().GetHash())
				writeRes.Status = &google_rpc_status.Status{
					Code:    int32(google_rpc_code.Code_INVALID_ARGUMENT),
					Message: fmt.Sprintf("Data to be written did not hash to given Digest"),
				}
				resultCh <- writeRes
				return
			}

			storeName := bazel.DigestStoreName(r.GetDigest())
			buffer := bytes.NewReader(r.GetData())
			writeErr := s.writeToStore(storeName, buffer)
			if writeErr != nil {
				writeRes.Status = &google_rpc_status.Status{
					Code:    int32(google_rpc_code.Code_INTERNAL),
					Message: writeErr.Error(),
				}
			} else {
				writeRes.Status = &google_rpc_status.Status{
					Code: int32(google_rpc_code.Code_OK),
				}
			}
			resultCh <- writeRes
		}(blobReq)
	}

	wg.Wait()
	close(resultCh)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Infof("Finished processing CAS BatchUpdateBlobs Request of length: %d", length)
	return res, err
}

// BatchReadBlobs reads a batch of blobs from bundlestore with parallelism and returns them inline in the response
func (s *casServer) BatchReadBlobs(
	ctx context.Context,
	req *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	log.Debugf("Received CAS BatchReadBlobs request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	var length int64 = 0
	var err error = nil

	// Record metrics based on final error condition
	defer func() {
		if err == nil {
			s.stat.Counter(stats.BzBatchReadSuccessCounter).Inc(1)
			if length > 0 {
				s.stat.Histogram(stats.BzBatchReadLengthHistogram).Update(length)
			}
		} else {
			s.stat.Counter(stats.BzBatchReadFailureCounter).Inc(1)
		}
	}()
	defer s.stat.Latency(stats.BzBatchReadLatency_ms).Time().Stop()

	sem := make(chan struct{}, BatchParallelism)
	resultCh := make(chan *remoteexecution.BatchReadBlobsResponse_Response)
	var wg sync.WaitGroup
	res := &remoteexecution.BatchReadBlobsResponse{}
	length = int64(len(req.GetDigests()))
	log.Infof("Processing CAS BatchReadBlobs request of length: %d", length)

	go func() {
		for r := range resultCh {
			res.Responses = append(res.GetResponses(), r)
			<-sem
			wg.Done()
		}
	}()

	// Perform operations in goroutines
	for _, digest := range req.GetDigests() {
		if err != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(d *remoteexecution.Digest) {
			readRes := &remoteexecution.BatchReadBlobsResponse_Response{Digest: d}

			// Return empty data if we got a request to read the empty sha
			if d.GetHash() == bazel.EmptySha {
				readRes.Status = &google_rpc_status.Status{
					Code: int32(google_rpc_code.Code_OK),
				}
				resultCh <- readRes
				return
			}

			// Read and return result. We interpret read errors as Not Found
			storeName := bazel.DigestStoreName(d)
			r, _, openErr := s.storeConfig.Store.OpenForRead(storeName)
			if openErr != nil {
				readRes.Status = &google_rpc_status.Status{
					Code: int32(google_rpc_code.Code_NOT_FOUND),
				}
				resultCh <- readRes
				return
			}

			asBytes, readErr := ioutil.ReadAll(r)
			if readErr != nil {
				log.Errorf("Error reading data for digest %s: %s", d.GetHash(), readErr)
				readRes.Status = &google_rpc_status.Status{
					Code:    int32(google_rpc_code.Code_INTERNAL),
					Message: fmt.Sprintf("Error reading data for digest %s: %s", d.GetHash(), readErr),
				}
				resultCh <- readRes
				return
			}
			defer r.Close()

			if int64(len(asBytes)) != d.GetSizeBytes() {
				readRes.Status = &google_rpc_status.Status{
					Code:    int32(google_rpc_code.Code_INVALID_ARGUMENT),
					Message: fmt.Sprintf("Data length mismatch - read %d for Digest %s/%d", len(asBytes), d.GetHash(), d.GetSizeBytes()),
				}
				resultCh <- readRes
				return
			}
			readRes.Data = asBytes
			readRes.Status = &google_rpc_status.Status{
				Code: int32(google_rpc_code.Code_OK),
			}
			resultCh <- readRes
		}(digest)
	}

	wg.Wait()
	close(resultCh)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	log.Infof("Finished processing CAS BatchReadBlobs Request of length: %d", length)
	return res, err
}

// GetTree not supported in Scoot
func (s *casServer) GetTree(
	req *remoteexecution.GetTreeRequest, gtServer remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "Currently unsupported in Scoot")
}

// ByteStream APIs

// Serves content in the bundlestore to a client via grpc streaming.
// Implements googleapis bytestream Read
func (s *casServer) Read(req *bytestream.ReadRequest, ser bytestream.ByteStream_ReadServer) error {
	log.Debugf("Received CAS Read request: %s", req)

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
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
		r, _, err = s.storeConfig.Store.OpenForRead(storeName)
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

	// Verify committed length with Digest size
	if committed != resource.Digest.GetSizeBytes() {
		log.Errorf("Data length/digest mismatch: %d/%d", committed, resource.Digest.GetSizeBytes())
		return status.Error(codes.InvalidArgument, fmt.Sprintf("Data to be written len: %d mismatch with request Digest size: %d", committed, resource.Digest.GetSizeBytes()))
	}
	// Verify buffer SHA with Digest SHA
	if bufferHash := fmt.Sprintf("%x", sha256.Sum256(buffer.Bytes())); bufferHash != resource.Digest.GetHash() {
		log.Errorf("Data hash/digest hash mismatch: %s/%s", bufferHash, resource.Digest.GetHash())
		return status.Error(codes.InvalidArgument, fmt.Sprintf("Data to be written did not hash to given Digest"))
	}

	// Write to underlying Store
	err = s.writeToStore(storeName, buffer)
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

// QueryWriteStatus not supported in Scoot
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

	r, _, err := s.storeConfig.Store.OpenForRead(address.storeName)
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
	// Use CAS Default TTL setting until API supports cache priority settings
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

func (s *casServer) writeToStore(name string, data io.Reader) error {
	// Using CAS Default TTL setting until API supports cache priority settings
	ttl := store.GetTTLValue(s.storeConfig.TTLCfg)
	if ttl != nil {
		ttl.TTL = time.Now().Add(DefaultTTL)
	}
	if err := s.storeConfig.Store.Write(name, data, ttl); err != nil {
		return err
	}
	return nil
}

// Interface for reading Empty data in a normal way while bypassing the underlying store
type nilReader struct{}

func (n *nilReader) Read([]byte) (int, error) { return 0, io.EOF }
func (n *nilReader) Close() error             { return nil }
