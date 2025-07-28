package sdk

import (
	"context"

	"github.com/kokaq/protocol/proto"
)

type KokaqReceiverClient struct {
	queue *KokaqQueueClient
}

type MessageHandler func(ctx context.Context, msgs []*proto.KokaqMessageResponse) error

func (r *KokaqReceiverClient) PollAndProcess(ctx context.Context, count uint32, fn MessageHandler) error {
	resp, err := r.queue.data.Dequeue(ctx, &proto.DequeueRequest{
		Namespace: r.queue.namespace,
		Queue:     r.queue.queue,
		MaxCount:  count,
	})
	if err != nil {
		return err
	}

	if resp.Messages == nil {
		return nil // No message to process
	}

	return fn(ctx, resp.Messages)
}

func (r *KokaqReceiverClient) PeekAndProcess(ctx context.Context, count int32, lockDurationSec uint32, handler func(ctx context.Context, msg *proto.KokaqMessageResponse, lockID string) error) error {
	resp, err := r.queue.data.PeekLock(ctx, &proto.PeekLockRequest{
		Namespace:    r.queue.namespace,
		Queue:        r.queue.queue,
		LockDuration: lockDurationSec,
	})
	if err != nil {
		return err
	}

	for _, locked := range resp.Locked {
		err := handler(ctx, locked.Message, locked.LockId)
		if err != nil {
			// User may decide to ack/nack manually
			return err
		}
	}

	return nil
}

func (r *KokaqReceiverClient) Ack(ctx context.Context, msgID, lockID string) error {
	_, err := r.queue.data.Ack(ctx, &proto.AckRequest{
		Namespace: r.queue.namespace,
		Queue:     r.queue.queue,
		MessageId: msgID,
		LockId:    lockID,
	})
	return err
}

func (r *KokaqReceiverClient) Nack(ctx context.Context, msgID, lockID string, reason proto.FailureReason, requeue bool) error {
	_, err := r.queue.data.Nack(ctx, &proto.NackRequest{
		Namespace:          r.queue.namespace,
		Queue:              r.queue.queue,
		MessageId:          msgID,
		LockId:             lockID,
		FailureReason:      reason,
		RequeueImmediately: requeue,
	})
	return err
}

func (r *KokaqReceiverClient) Release(ctx context.Context, msgID, lockID string, makeVisibleNow bool) error {
	_, err := r.queue.data.ReleaseLock(ctx, &proto.ReleaseLockRequest{
		Namespace:      r.queue.namespace,
		Queue:          r.queue.queue,
		MessageId:      msgID,
		LockId:         lockID,
		MakeVisibleNow: makeVisibleNow,
	})
	return err
}

func (r *KokaqReceiverClient) ExtendVisibility(ctx context.Context, msgID, lockID string, extraMs uint32) error {
	_, err := r.queue.data.Extend(ctx, &proto.ExtendVisibilityTimeoutRequest{
		Namespace:    r.queue.namespace,
		Queue:        r.queue.queue,
		MessageId:    msgID,
		LockId:       lockID,
		AdditionalMs: extraMs,
	})
	return err
}

func (r *KokaqReceiverClient) RefreshVisibility(ctx context.Context, msgID, lockID string) error {
	_, err := r.queue.data.RefreshVisibilityTimeout(ctx, &proto.RefreshVisibilityTimeoutRequest{
		Namespace: r.queue.namespace,
		Queue:     r.queue.queue,
		MessageId: msgID,
		LockId:    lockID,
	})
	return err
}

func (r *KokaqReceiverClient) SetVisibility(ctx context.Context, msgID, lockID string, timeoutMs uint32) error {
	_, err := r.queue.data.SetVisibilityTimeout(ctx, &proto.SetVisibilityTimeoutRequest{
		Namespace:    r.queue.namespace,
		Queue:        r.queue.queue,
		MessageId:    msgID,
		LockId:       lockID,
		NewTimeoutMs: timeoutMs,
	})
	return err
}
