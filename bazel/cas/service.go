// Bazel Remote Execution API gRPC server
// Contains limited implementation of the ContentAddressableStore API interface
package cas

import (
	"net"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	googlebytestream "google.golang.org/genproto/googleapis/bytestream"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Implements GRPCServer and remoteexecution.ContentAddressableStoreServer interfaces
type casServer struct {
	listener net.Listener
	server   *grpc.Server
}

// Creates a new GRPCServer (CASServer, ByteStreamServer) based on a listener, and preregisters the service
func NewCASServer(l net.Listener) *casServer {
	g := casServer{listener: l, server: grpc.NewServer()}
	remoteexecution.RegisterContentAddressableStorageServer(g.server, &casServer{})
	googlebytestream.RegisterByteStreamServer(g.server, &casServer{})
	return &g
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
	res := remoteexecution.FindMissingBlobsResponse{}
	// Return all requested blobs as missing
	res.MissingBlobDigests = make([]*remoteexecution.Digest, len(req.BlobDigests), len(req.BlobDigests))
	copy(res.MissingBlobDigests, req.BlobDigests)
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
func (s *casServer) Read(req *googlebytestream.ReadRequest, ser googlebytestream.ByteStream_ReadServer) error {
	log.Infof("Received CAS Read request: %s", req)
	// Real implementation: fetch resource from backend and call Send(Data []byte) until finished
	return nil
}

// Writes data into bundlestore from a client via grpc streaming. Clients can use this API
// to determine the progress of data being committed and to resume writes after interruptions.
// Implements googleapis bytestream Write
func (s *casServer) Write(googlebytestream.ByteStream_WriteServer) error {
	log.Info("Received CAS Write request")
	// Real implementation: stream from client with Recv(), finish with SendAndClose
	return nil
}

// QueryWriteStatus gives status information about a Write operation in progress
func (s *casServer) QueryWriteStatus(
	ctx context.Context,
	req *googlebytestream.QueryWriteStatusRequest) (*googlebytestream.QueryWriteStatusResponse, error) {
	log.Infof("Received CAS QueryWriteStatus request: %s", req)
	// Placeholder - response indicates size 0 resource in completed state
	res := googlebytestream.QueryWriteStatusResponse{CommittedSize: 0, Complete: true}
	return &res, nil
}
