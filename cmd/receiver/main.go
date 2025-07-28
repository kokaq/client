package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/kokaq/client/sdk"
	"github.com/kokaq/protocol/proto"
)

func main() {
	// Create a context with a 2-second timeout
	ctx := context.Background()
	client, err := sdk.NewKokaqClient(":9001", &sdk.KokaqClientOptions{})
	if err != nil {
		fmt.Printf("%s", err)
	}
	_, err = client.CreateQueue(ctx, "test1", "test1")
	if err != nil {
		fmt.Printf("%s", err)
	}
	qclient, err := client.GetQueueClient(ctx, "test1", "test1")
	if err != nil {
		fmt.Printf("%s", err)
	}
	receiver := qclient.Receiver()

	var stopFlag int32 = 0

	// Launch a goroutine to wait for Enter key
	go func() {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Press ENTER to stop the loop...")
		_, _ = reader.ReadString('\n') // blocks until Enter is pressed
		atomic.StoreInt32(&stopFlag, 1)
	}()

	for {

		if atomic.LoadInt32(&stopFlag) == 1 {
			fmt.Println("Exiting loop.")
			break
		}
		receiver.PeekAndProcess(ctx, 1, 0, peekHandler)
		receiver.PollAndProcess(ctx, 1, pollHandler)
		if err != nil {
			fmt.Printf("%s", err)
		}
	}
}

func peekHandler(ctx context.Context, msg *proto.KokaqMessageResponse, lockID string) error {
	fmt.Printf("Peek message: %s, Priority: %d\n", msg.Message.MessageId, msg.Message.Priority)
	return nil
}

func pollHandler(ctx context.Context, msgs []*proto.KokaqMessageResponse) error {
	for _, msg := range msgs {
		fmt.Printf("Dequeue message: %s, Priority: %d\n", msg.Message.MessageId, msg.Message.Priority)
	}
	return nil
}
