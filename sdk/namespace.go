package sdk

import (
	"context"

	"github.com/kokaq/protocol/proto"
)

type KokaqNamespaceClient struct {
	client    *KokaqClient
	namespace string
}

func (n *KokaqNamespaceClient) Create(ctx context.Context) error {
	_, err := n.client.control.AddNamespace(ctx, &proto.KokaqNamespaceRequest{
		Namespace: n.namespace,
	})
	return err
}

func (n *KokaqNamespaceClient) Delete(ctx context.Context) error {
	_, err := n.client.control.DeleteNamespace(ctx, &proto.KokaqNamespaceRequest{
		Namespace: n.namespace,
	})
	return err
}

func (n *KokaqNamespaceClient) Stats(ctx context.Context) (map[string]uint64, error) {
	resp, err := n.client.control.GetStats(ctx, &proto.KokaqNamespaceRequest{
		Namespace: n.namespace,
	})
	if err != nil {
		return nil, err
	}
	return resp.Stats, nil
}
