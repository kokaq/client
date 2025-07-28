package sdk

import (
	"context"
	"crypto/tls"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type AuthTokenProvider func() (string, error)

func tokenAuthInterceptor(provider AuthTokenProvider) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		token, err := provider()
		if err != nil {
			return err
		}
		md := metadata.New(map[string]string{"authorization": "Bearer " + token})
		ctx = metadata.NewOutgoingContext(ctx, md)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

type KokaqClientOptions struct {
	TLSEnabled   bool
	TLSConfig    *tls.Config
	TokenSource  AuthTokenProvider             // Optional: returns token
	DialTimeout  time.Duration                 // Optional: dial timeout
	Interceptors []grpc.UnaryClientInterceptor // Extra user-supplied interceptors
}
