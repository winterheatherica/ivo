// Package grpcx sets up gRPC servers and clients, and errors with an
// ErrorInfo.reason that the gateway reads as the error code for the app.
package grpcx

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// NewServer creates a gRPC server with panic recovery, short logs, and a health check.
func NewServer(log *slog.Logger) *grpc.Server {
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(recoverer(log), logger(log)))
	healthpb.RegisterHealthServer(srv, health.NewServer())
	return srv
}

// Serve runs srv on addr until ctx is cancelled.
func Serve(ctx context.Context, srv *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Dial creates a client connection. M1 uses plaintext inside the Compose network;
// client-credentials tokens and NetworkPolicy are added in M3 and M5.
func Dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Fail creates a gRPC error with an ErrorInfo.reason.
func Fail(code codes.Code, reason, msg string) error {
	st := status.New(code, msg)
	if withInfo, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "ivo"}); err == nil {
		st = withInfo
	}
	return st.Err()
}

// Reason extracts the ErrorInfo.reason from a gRPC error, or "" if there is none.
func Reason(err error) string {
	for _, d := range status.Convert(err).Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

// Internal logs err and returns a gRPC Internal error without details.
func Internal(log *slog.Logger, method string, err error) error {
	log.Error("internal error", "method", method, "err", err)
	return status.Error(codes.Internal, "internal error")
}

func recoverer(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in gRPC handler", "method", info.FullMethod, "panic", r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func logger(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		level := slog.LevelDebug
		if code == codes.Internal || code == codes.Unknown {
			level = slog.LevelError
		}
		log.Log(ctx, level, "grpc", "method", info.FullMethod, "code", code.String(),
			"duration_ms", time.Since(start).Milliseconds())
		return resp, err
	}
}
