package server

import (
	"context"

	api "proglog/api/v1"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Authorizer decides whether an authenticated subject may perform a log action.
type Authorizer interface {
	// Authorize returns an error when the requested permission is denied.
	Authorize(subject, object, action string) error
}

// Config supplies the persistent log and authorization policy required by RPC handlers.
type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

// All RPC operations target the literal policy object "*" with an action per operation.
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

// CommitLog is the storage contract needed by unary and streaming RPCs.
type CommitLog interface {
	// Append assigns an offset and stores the record.
	Append(*api.Record) (uint64, error)
	// Read retrieves an absolute offset or returns an error.
	Read(uint64) (*api.Record, error)
}

// Compile-time assertion that grpcServer implements the generated service contract.
var _ api.LogServer = (*grpcServer)(nil)

// grpcServer implements log RPCs using shared storage and authorization dependencies.
type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// newgrpcServer attaches dependencies; embedded generated methods supply fallback RPCs.
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// NewGRPCServer registers the log service with logging, authentication, and telemetry.
// The caller supplies transport credentials and starts serving on a listener.
// Trace sampling and metric view registration affect process-wide OpenCensus state.
func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64(
					"grpc.time_ns",
					duration.Nanoseconds(),
				)
			},
		),
	}
	trace.ApplyConfig(trace.Config{
		DefaultSampler: trace.AlwaysSample(),
	})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	// Tagging runs before logging, then authentication populates the handler context.
	grpcOpts = append(
		grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	gsrv := grpc.NewServer(grpcOpts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// Produce checks write permission before appending and returns the assigned offset.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume checks read permission before looking up the requested absolute offset.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{
		Record: record,
	}, nil
}

// ProduceStream receives, authorizes, appends, and acknowledges one record at a time.
// Receive errors, including client EOF, are returned directly.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream follows offsets from the request until cancellation or an error.
// It retries missing offsets immediately, so waiting at the log tail busy-polls.
func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			// Advance only after delivery so a missing offset is retried without skipping data.
			req.Offset++
		}
	}
}

// subjectContextKey avoids collisions with context values from other packages.
type subjectContextKey struct{}

// authenticate stores the verified TLS leaf certificate common name as the subject.
// Connections without transport auth get an empty subject for policy evaluation;
// authenticated connections are assumed to use TLS with a verified certificate chain.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// subject retrieves the identity installed by the authentication interceptor.
// Calling it without that interceptor context panics.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}
