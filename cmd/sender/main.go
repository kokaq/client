package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/kokaq/client/sdk"
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
	sender := qclient.Sender()

	var stopFlag int32 = 0

	// Launch a goroutine to wait for Enter key
	go func() {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Press ENTER to stop the loop...")
		_, _ = reader.ReadString('\n') // blocks until Enter is pressed
		atomic.StoreInt32(&stopFlag, 1)
	}()
	counter := 0
	rand.Seed(time.Now().UnixNano())
	for {
		if atomic.LoadInt32(&stopFlag) == 1 {
			fmt.Println("Exiting loop.")
			break
		}
		msgId, enqueuedAt, err := sender.Send(ctx, []byte(fmt.Sprintf("test-%d", counter)), rand.Uint64())
		if err != nil {
			fmt.Printf("%s", err)
		} else {
			fmt.Printf("%s %s", msgId, enqueuedAt)
		}
		counter++
		if counter%10 == 0 {
			time.Sleep(5000 * time.Millisecond)
		}
	}
}
