// Bazel Remote Execution API gRPC server
// Contains limited implementation of the ContentAddressableStore API interface
package cas

import (
	"fmt"
	"io"
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

	// req.InstanceName currently ignored
	res := remoteexecution.FindMissingBlobsResponse{}
	if s.storeConfig == nil {
		return nil, status.Error(codes.Internal, "Internal Store not initialized")
	}

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

	resource, err := bazel.ParseResource(req.GetResourceName())
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("%v", err))
	}

	if req.GetReadOffset() < 0 || req.GetReadOffset() >= resource.Digest.GetSizeBytes() {
		return status.Error(codes.OutOfRange, fmt.Sprintf("Invalid read offset %d for size %d", req.GetReadOffset(), resource.Digest.GetSizeBytes()))
	}
	if req.GetReadLimit() < 0 {
		return status.Error(codes.InvalidArgument, "Read limit < 0 invalid")
	}

	storeName := bazel.DigestStoreName(resource.Digest)

	// TODO register with casServer inflight request tracker?
	log.Infof("Opening store resource for reading: %s", storeName)
	r, err := s.storeConfig.Store.OpenForRead(storeName)
	if err != nil {
		return status.Error(codes.Internal, fmt.Sprintf("Store failed opening resource for read: %s: %v", storeName, err))
	}
	defer r.Close()

	res := &bytestream.ReadResponse{}
	p := make([]byte, bazel.MaxCASBufferSize)
	for {
		_, err := r.Read(p)
		if err == io.EOF {
			break
		} else if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("Store failed reading from %s: %v", storeName, err))
		}

		res.Data = p
		err = ser.Send(res)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("Failed to send ReadResponse: %v", err))
		}
		res.Reset()
	}

	return nil
}

// Writes data into bundlestore from a client via grpc streaming. Clients can use this API
// to determine the progress of data being committed and to resume writes after interruptions.
// Implements googleapis bytestream Write
func (s *casServer) Write(bytestream.ByteStream_WriteServer) error {
	log.Info("Received CAS Write request")

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}

	// TODO Real implementation: stream from client with Recv(), finish with SendAndClose
	return nil
}

// QueryWriteStatus gives status information about a Write operation in progress
func (s *casServer) QueryWriteStatus(
	ctx context.Context,
	req *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	log.Infof("Received CAS QueryWriteStatus request: %s", req)

	if !s.IsInitialized() {
		return nil, status.Error(codes.Internal, "Server not initialized")
	}

	// TODO Placeholder - response indicates size 0 resource in completed state
	res := bytestream.QueryWriteStatusResponse{CommittedSize: 0, Complete: true}
	return &res, nil
}
