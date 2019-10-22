// Bazel Remote Execution API gRPC
package bazel

import (
	"net"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"golang.org/x/net/netutil"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"

	"github.com/twitter/scoot/common/grpchelpers"
)

// Wrapping interface for gRPC servers to work seamlessly with magicbag semantics

// gRPC server interface encapsulating gRPC operations and execution server,
// intended to reduce gRPC listener and registration boilerplate.
type GRPCServer interface {
	Serve() error
}

// GRPCConfig holds fields used for configuring startup of GRPC Listeners and Servers
// Zero value integer fields are interpretted as unlimited
type GRPCConfig struct {
	GRPCAddr          string // Required: ip:port the Listener will bind to
	ListenerMaxConns  int    // Maximum simultaneous connections the listener will accept
	RateLimitPerSec   int    // Maximum incoming requests per second
	BurstLimitPerSec  int    // Maximum per-burst incoming requests per second (within RateLimitPerSec)
	ConcurrentStreams int    // Maximum concurrent GRPC streams allowed per client
	MaxConnIdleMins   int    // Maximum time a connection can remain open until the server closes it
	ConcurrentReqSize int64  // Size of resources used in concurrent requests, in bytes (Internal usage)
}

// Creates a new net.Listener with the configured address and limits
func (c *GRPCConfig) NewListener() (net.Listener, error) {
	listener, err := net.Listen("tcp", c.GRPCAddr)
	if err != nil {
		return nil, err
	}
	if c.ListenerMaxConns > 0 {
		log.Infof("Creating LimitListener with max: %d", c.ListenerMaxConns)
		limitListener := netutil.LimitListener(listener, c.ListenerMaxConns)
		return limitListener, nil
	}
	return listener, nil
}

// Creates a new *grpc.Server configured with ServerOptions based on the GRPCConfig fields
func (c *GRPCConfig) NewGRPCServer() *grpc.Server {
	var serverOpts []grpc.ServerOption

	// 0 is a valid Limiter that rejects all requests, but that's not useful, so we interpret 0 as unlimited
	if c.RateLimitPerSec > 0 && c.BurstLimitPerSec > 0 {
		log.Infof("Creating Limiter with rate/burst: %d/%d", c.RateLimitPerSec, c.BurstLimitPerSec)
		limiterOpt := grpc.InTapHandle(NewTap(c.RateLimitPerSec, c.BurstLimitPerSec).Handler)
		serverOpts = append(serverOpts, limiterOpt)
	}

	if c.ConcurrentStreams > 0 {
		log.Infof("Setting concurrent streams limit: %d", c.ConcurrentStreams)
		streamsOpt := grpc.MaxConcurrentStreams(uint32(c.ConcurrentStreams))
		serverOpts = append(serverOpts, streamsOpt)
	}

	if c.MaxConnIdleMins > 0 {
		log.Infof("Setting KeepaliveParams: max connection idle mins: %d", c.MaxConnIdleMins)
		kpOpt := grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: time.Duration(c.MaxConnIdleMins) * time.Minute,
		})
		serverOpts = append(serverOpts, kpOpt)
	}

	log.Infof("setting MaxRecvMsgSize: %d", BatchMaxCombinedSize)
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(BatchMaxCombinedSize))

	log.Infof("setting MaxSendMsgSize: %d", BatchMaxCombinedSize)
	serverOpts = append(serverOpts, grpc.MaxSendMsgSize(BatchMaxCombinedSize))

	return grpchelpers.NewServer(serverOpts...)
}

// Encapsulates a rate-per-second limiter that will check all incoming requests (per-connection goroutine)
// Handle func Fulfills grpc/tap.ServerInHandle
type TapLimiter struct {
	limiter *rate.Limiter
}

// Create a new TapLimiter with specified rate and burst allowance
func NewTap(maxRequests, maxBurst int) *TapLimiter {
	return &TapLimiter{
		limiter: rate.NewLimiter(rate.Limit(maxRequests), maxBurst),
	}
}

// Wait until the Limiter allows the a request or the Context expires
// Client sees non-nil err as an RPC error with code=Unavailable, desc includes RST_STREAM or REFUSED_STREAM
func (t *TapLimiter) Handler(ctx context.Context, info *tap.Info) (context.Context, error) {
	if err := t.limiter.Wait(ctx); err != nil {
		log.Warnf("Tap limiter dropped connection due to rate limit: %s. Incoming request: %s", err, info.FullMethodName)
		return nil, status.Error(codes.ResourceExhausted, "Resource exhausted due to rate limit")
	}
	return ctx, nil
}
