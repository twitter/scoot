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

// Implements GRPCServer, remoteexecution.ContentAddressableStoreServer, bytestream.ByteStreamServer interfaces
type casServer struct {
	listener    net.Listener
	server      *grpc.Server
	storeConfig *store.StoreConfig
}

// Creates a new GRPCServer (CASServer/ByteStreamServer) based on a listener, and preregisters the service
func MakeCASServer(l net.Listener, cfg *store.StoreConfig) *casServer {
	g := casServer{
		listener:    l,
		server:      grpchelpers.NewServer(),
		storeConfig: cfg,
	}
	remoteexecution.RegisterContentAddressableStorageServer(g.server, &g)
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
	res := remoteexecution.FindMissingBlobsResponse{}

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
	ctx context.Context,
	req *remoteexecution.GetTreeRequest) (*remoteexecution.GetTreeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot")
}

// CAS - ByteStream APIs

// Serves content in the bundlestore to a client via grpc streaming.
// Implements googleapis bytestream Read
func (s *casServer) Read(req *bytestream.ReadRequest, ser bytestream.ByteStream_ReadServer) error {
	log.Debugf("Received CAS Read request: %s", req)

	if !s.IsInitialized() {
		return status.Error(codes.Internal, "Server not initialized")
	}

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
			log.Errorf("Failed to OpenForRead: %v", err)
			return status.Error(codes.NotFound, fmt.Sprintf("Failed opening resource %s for read, returning NotFound. Err: %v", storeName, err))
		}
	}
	defer r.Close()

	res := &bytestream.ReadResponse{}
	c := int64(DefaultReadCapacity)
	length := int64(0)

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
// TODO store.Stores do not support partial Writes, and neither does our implementation.
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
	var resource *Resource = nil
	resourceName, storeName := "", ""
	var err error

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
				res := &bytestream.WriteResponse{CommittedSize: bazel.EmptySize}
				err := ser.SendAndClose(res)
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
		if wr.GetWriteOffset() > 0 {
			log.Error("Invalid write offset")
			return status.Error(codes.Unimplemented, "Currently unsupported in Scoot - Writes are not resumable")
		}

		buffer.Write(wr.GetData())

		// Per API, client indicates all data has been sent
		if wr.GetFinishWrite() {
			break
		}
	}

	// Get committed length and verify - Digest size can be arbitrarily set by the client, but is a trusted value after insertion
	committed := int64(buffer.Len())
	if committed != resource.Digest.GetSizeBytes() {
		log.Errorf("Data length/digest mismatch: %d/%d", committed, resource.Digest.GetSizeBytes())
		return status.Error(codes.Internal, fmt.Sprintf("Data to be written len: %d mismatch with request Digest size: %d", committed, resource.Digest.GetSizeBytes()))
	}

	// Write to underlying Store
	ttl := store.GetTTLValue(s.storeConfig.TTLCfg)
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
// TODO unsupported, may be added later for V1
func (s *casServer) QueryWriteStatus(
	context.Context, *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Currently unsupported in Scoot - Writes are not resumable")
}

// Interface for reading Empty data in a normal way while bypassing the underlying store
type nilReader struct{}

func (n *nilReader) Read([]byte) (int, error) { return 0, io.EOF }
func (n *nilReader) Close() error             { return nil }
