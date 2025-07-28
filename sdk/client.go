package sdk

import (
	"context"
	"fmt"

	"github.com/kokaq/protocol/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type KokaqClient struct {
	conn    *grpc.ClientConn
	control proto.KokaqControlPlaneClient
	options *KokaqClientOptions
}

func NewKokaqClient(serverAddr string, opts *KokaqClientOptions) (*KokaqClient, error) {
	var grpcOpts []grpc.DialOption

	// TLS support
	var grpcTransCreds credentials.TransportCredentials
	if opts != nil && opts.TLSEnabled {
		grpcTransCreds = credentials.NewTLS(opts.TLSConfig)
	} else {
		grpcTransCreds = insecure.NewCredentials()
	}
	grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(grpcTransCreds))

	// Auth interceptor (token-based)
	if opts != nil && opts.TokenSource != nil {
		grpcOpts = append(grpcOpts, grpc.WithUnaryInterceptor(tokenAuthInterceptor(opts.TokenSource)))
	}

	// Extra interceptors
	if opts != nil {
		for _, i := range opts.Interceptors {
			grpcOpts = append(grpcOpts, grpc.WithUnaryInterceptor(i))
		}
	}

	conn, err := grpc.NewClient(serverAddr, grpcOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}
	return &KokaqClient{
		conn:    conn,
		control: proto.NewKokaqControlPlaneClient(conn),
		options: opts,
	}, nil
}

// Control operations
func (kc *KokaqClient) CreateNamespace(ctx context.Context, name string) (*proto.KokaqNamespaceResponse, error) {
	return kc.control.AddNamespace(ctx, &proto.KokaqNamespaceRequest{
		Namespace: name,
	})
}

func (kc *KokaqClient) CreateQueue(ctx context.Context, ns string, queue string) (*proto.KokaqQueueResponse, error) {
	return kc.control.AddQueue(ctx, &proto.KokaqQueueRequest{
		Namespace:        ns,
		Queue:            queue,
		EnableDeadLetter: true,
	})
}

func (c *KokaqClient) GetNamespaceClient(ctx context.Context, namespace string) (*KokaqNamespaceClient, error) {
	return &KokaqNamespaceClient{
		client:    c,
		namespace: namespace,
	}, nil
}

func (c *KokaqClient) GetQueueClient(ctx context.Context, namespace, queue string) (*KokaqQueueClient, error) {

	// This talks to control plane and asks for the data endpoint
	// resolve this client

	var grpcOpts []grpc.DialOption

	// TLS support
	var grpcTransCreds credentials.TransportCredentials
	if c.options != nil && c.options.TLSEnabled {
		grpcTransCreds = credentials.NewTLS(c.options.TLSConfig)
	} else {
		grpcTransCreds = insecure.NewCredentials()
	}
	grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(grpcTransCreds))

	// Auth interceptor (token-based)
	if c.options != nil && c.options.TokenSource != nil {
		grpcOpts = append(grpcOpts, grpc.WithUnaryInterceptor(tokenAuthInterceptor(c.options.TokenSource)))
	}

	// Extra interceptors
	if c.options != nil {
		for _, i := range c.options.Interceptors {
			grpcOpts = append(grpcOpts, grpc.WithUnaryInterceptor(i))
		}
	}

	res, err := c.control.GetDataplane(ctx, &proto.GetDataplaneRequest{
		Namespace: namespace,
		Queue:     queue,
	})

	if err != nil {
		return nil, err
	}

	conn, err := grpc.NewClient(res.Address, grpcOpts...)

	if err != nil {
		return nil, err
	}

	return &KokaqQueueClient{
		client:    c,
		namespace: namespace,
		queue:     queue,
		data:      proto.NewKokaqDataPlaneClient(conn),
	}, nil
}

func (c *KokaqClient) Close() error {
	return c.conn.Close()
}
