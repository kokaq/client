package sdk

import (
	"context"

	"github.com/kokaq/protocol/proto"
)

type KokaqQueueClient struct {
	client    *KokaqClient
	namespace string
	queue     string
	data      proto.KokaqDataPlaneClient
}

func (q *KokaqQueueClient) Delete(ctx context.Context) error {
	_, err := q.client.control.DeleteQueue(ctx, &proto.KokaqQueueRequest{
		Namespace: q.namespace,
		Queue:     q.queue,
	})
	return err
}

func (q *KokaqQueueClient) Clear(ctx context.Context) error {
	_, err := q.client.control.ClearQueue(ctx, &proto.KokaqQueueRequest{
		Namespace: q.namespace,
		Queue:     q.queue,
	})
	return err
}

func (q *KokaqQueueClient) Sender() *KokaqSenderClient {
	return &KokaqSenderClient{queue: q}
}

func (q *KokaqQueueClient) Receiver() *KokaqReceiverClient {
	return &KokaqReceiverClient{queue: q}
}
