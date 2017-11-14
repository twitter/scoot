// Bazel Remote Execution API gRPC server
// Contains limited implementation of the ContentAddressableStore API interface
package cas

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/common/grpchelpers"
	"github.com/twitter/scoot/snapshot/store"
)

// Implements GRPCServer and remoteexecution.ContentAddressableStoreServer interfaces
type casServer struct {
	listener    net.Listener
	server      *grpc.Server
	storeConfig *store.StoreConfig
}

// Creates a new GRPCServer (CASServer, ByteStreamServer) based on a listener, and preregisters the service
func MakeCASServer(l net.Listener, cfg *store.StoreConfig) *casServer {
	g := casServer{
		listener:    l,
		server:      grpchelpers.NewServer(),
		storeConfig: cfg,
	}
	remoteexecution.RegisterContentAddressableStorageServer(g.server, &casServer{})
	bytestream.RegisterByteStreamServer(g.server, &casServer{})
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
	log.Infof("Received CAS FindMissingBlobs request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}
	res := remoteexecution.FindMissingBlobsResponse{}

	for _, digest := range req.GetBlobDigests() {
		storeName := bazel.DigestStoreName(digest)
		exists, err := s.storeConfig.Store.Exists(storeName)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("Store failed checking existence of %s: %v", storeName, err))
		}
		if !exists {
			res.MissingBlobDigests = append(res.MissingBlobDigests, digest)
		}
	}

	return &res, nil
}

// BatchUpdate not supported in Scoot for V1
func (s *casServer) BatchUpdateBlobs(
	ctx context.Context,
	req *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot - update blobs independently")
}

// DEPRECATED - Included for protobuf generated code compatability/compilation
func (s *casServer) GetTree(
	ctx context.Context,
	req *remoteexecution.GetTreeRequest) (*remoteexecution.GetTreeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "This API is marked as deprecated in the Bazel API definition")
}

// CAS - ByteStream APIs

// Serves content in the bundlestore to a client via grpc streaming.
// Implements googleapis bytestream Read
func (s *casServer) Read(req *bytestream.ReadRequest, ser bytestream.ByteStream_ReadServer) error {
	log.Infof("Received CAS Read request: %s", req)

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}

	// Parse resource name per Bazel API specification
	resource, err := ParseReadResource(req.GetResourceName())
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("%v", err))
	}

	// Input validation per API spec
	if req.GetReadOffset() < 0 || req.GetReadOffset() >= resource.Digest.GetSizeBytes() {
		return status.Error(codes.OutOfRange, fmt.Sprintf("Invalid read offset %d for size %d", req.GetReadOffset(), resource.Digest.GetSizeBytes()))
	}
	if req.GetReadLimit() < 0 {
		return status.Error(codes.InvalidArgument, "Read limit < 0 invalid")
	}

	// Map digest to underlying store name
	storeName := bazel.DigestStoreName(resource.Digest)

	log.Infof("Opening store resource for reading: %s", storeName)
	r, err := s.storeConfig.Store.OpenForRead(storeName)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Store failed opening resource for read: %s: %v", storeName, err))
	}
	defer r.Close()

	res := &bytestream.ReadResponse{}
	c := resource.Digest.GetSizeBytes() - req.GetReadOffset()

	// If ReadOffset was specified, discard bytes prior to it
	if req.GetReadOffset() > 0 {
		_, err = io.CopyN(ioutil.Discard, r, req.GetReadOffset())
		if err != nil {
			return status.Error(codes.Internal, "Failed to read until offset")
		}
		c = c - req.GetReadOffset()
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

		if n > 0 {
			res.Data = p
			err = ser.Send(res)
			if err != nil {
				return status.Error(codes.Internal, fmt.Sprintf("Failed to send ReadResponse: %v", err))
			}
			res.Reset()
		}

		if err == nil {
			continue
		} else if err == io.EOF {
			break
		} else {
			return status.Error(codes.Internal, fmt.Sprintf("Failed to read from Store: %v", err))
		}
	}
	return nil
}

// Writes data into bundlestore from a client via grpc streaming.
// Implements googleapis bytestream Write
// TODO store.Stores do not support partial Writes, and neither does our implementation.
// We can support partial Write by keeping buffers for inflight requests in the casServer.
// When the entire Write is buffered, we can Write to the Store and return a response with the result.
func (s *casServer) Write(ser bytestream.ByteStream_WriteServer) error {
	log.Info("Received CAS Write request")

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}

	var p []byte
	var buff *bytes.Buffer
	var resource *Resource = nil
	resourceName, storeName := "", ""
	var err error

	// As indicated above, not supporting partial/resumable Writes for now.
	// Reads in a stream of data from the client, and proceeds when we've gotten it all.
	for {
		wr, err := ser.Recv()
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("Failed to Recv: %v", err))
		}

		// Set up resource on initial WriteRequest. Initialize read buffer based on Digest size
		if resource == nil {
			resourceName = wr.GetResourceName()

			resource, err = ParseWriteResource(resourceName)
			if err != nil {
				return status.Error(codes.InvalidArgument, fmt.Sprintf("%v", err))
			}
			p = make([]byte, 0, resource.Digest.GetSizeBytes())
			buff = bytes.NewBuffer(p)

			// TODO if data Exists, terminate immediately with size of existing data (Store is immutable)
		}

		// Validate subsequent WriteRequest fields
		if wr.GetResourceName() != "" && resourceName != wr.GetResourceName() {
			return status.Error(codes.InvalidArgument, fmt.Sprintf("ResourceName %s mismatch with previous %s", wr.GetResourceName(), resourceName))
		}

		buff.Write(wr.GetData())

		// Per API, client indicates all data has been sent
		if wr.GetFinishWrite() {
			break
		}
	}

	// Write to underlying Store
	storeName = bazel.DigestStoreName(resource.Digest)
	ttl := store.GetTTLValue(s.storeConfig.TTLCfg)
	err = s.storeConfig.Store.Write(storeName, buff, ttl)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Store failed writing to %s: %v", storeName, err))
	}

	res := &bytestream.WriteResponse{CommittedSize: int64(len(p))}
	err = ser.SendAndClose(res)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Failed to SendAndClose WriteResponse: %v", err))
	}

	return nil
}

// QueryWriteStatus gives status information about a Write operation in progress
// TODO unsupported, may be added later for V1
func (s *casServer) QueryWriteStatus(
	context.Context, *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot - Writes are not resumable")
}
