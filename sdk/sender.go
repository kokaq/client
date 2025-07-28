package sdk

import (
	"context"
	"time"

	"github.com/kokaq/protocol/proto"
)

type KokaqSenderClient struct {
	queue *KokaqQueueClient
}

func (s *KokaqSenderClient) Send(ctx context.Context, body []byte, priority uint64) (string, time.Time, error) {
	res, err := s.queue.data.Enqueue(ctx, &proto.EnqueueRequest{
		Message: &proto.KokaqMessageRequest{
			Queue:     s.queue.queue,
			Namespace: s.queue.namespace,

			Payload:  body,
			Priority: priority,
		},
	})
	return res.MessageId, res.EnqueuedAt.AsTime(), err
}
