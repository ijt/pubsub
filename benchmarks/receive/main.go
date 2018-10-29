// bench.go
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"cloud.google.com/go/pubsub"
	pubsublow "cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

const (
	subscriptionName = "projects/gocloud-212523/subscriptions/hits1"
	projectID        = "gocloud-212523"
)

var showingSendRates = flag.Bool("sendrates", false, "whether to show how many messages per second are being sent")

func main() {
	flag.Parse()
	ctx := context.Background()
	go send(ctx)
	benchmarkBatchReceive(ctx)
	benchmarkReceive(ctx)
}

func send(ctx context.Context) {
	client, err := pubsublow.NewPublisherClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for g := 0; g < 100; g++ {
		go pubWorker(ctx, client, 1000)
	}
}

func benchmarkBatchReceive(ctx context.Context) {
	fmt.Printf("\nBenchmarking batch receive.\n")
	client, err := pubsublow.NewSubscriberClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "# goroutines\tmsgs/sec\n")
	fmt.Fprintf(w, "------------\t--------\n")
	for _, ng := range []int{1, 10, 100} {
		var mu sync.Mutex
		msgCount := 0
		dt := clock(func() {
			var wg sync.WaitGroup
			for g := 0; g < ng; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					req := &pubsubpb.PullRequest{
						Subscription: subscriptionName,
						MaxMessages:  1000,
					}
					resp, err := client.Pull(ctx, req)
					if err != nil {
						log.Fatal(err)
					}
					mu.Lock()
					msgCount += len(resp.ReceivedMessages)
					mu.Unlock()

					var acks []string
					for _, m := range resp.ReceivedMessages {
						acks = append(acks, m.AckId)
					}
					ackReq := &pubsubpb.AcknowledgeRequest{
						Subscription: req.Subscription,
						AckIds:       acks,
					}
					if err := client.Acknowledge(ctx, ackReq); err != nil {
						log.Fatal(err)
					}
				}()
			}
			wg.Wait()
		})
		msgsPerSec := float64(msgCount) / dt.Seconds()
		fmt.Fprintf(w, "%d\t%.2g\n", ng, msgsPerSec)
	}
	w.Flush()
}

func benchmarkReceive(ctx context.Context) {
	fmt.Printf("\nBenchmarking receive on high-level GCP PubSub API.\n")
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	sub := client.Subscription("hits1")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	fmt.Fprintf(w, "# goroutines\tmsgs\tmsgs/sec\n")
	fmt.Fprintf(w, "------------\t----\t--------\n")
	for _, ng := range []int{1, 10, 100} {
		sub.ReceiveSettings.NumGoroutines = ng
		var mu sync.Mutex
		for _, nm := range []int{1, 10, 100, 1000} {
			msgCount := 0
			dt := clock(func() {
				cctx, cancel := context.WithCancel(ctx)
				err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
					defer msg.Ack()
					mu.Lock()
					defer mu.Unlock()
					msgCount++
					if msgCount >= nm {
						cancel()
					}
				})
				if err != nil {
					log.Fatal(err)
				}
			})
			msgsPerSec := float64(msgCount) / dt.Seconds()
			fmt.Fprintf(w, "%d\t%d\t%.2g\n", ng, nm, msgsPerSec)
		}
	}
	w.Flush()
}

var (
	sendsMu     sync.Mutex
	sendsPerSec = 0
)

func pubWorker(ctx context.Context, client *pubsublow.PublisherClient, batchSize int) {
	for {
		var ms []*pubsubpb.PubsubMessage
		for j := 0; j < batchSize; j++ {
			m := pubsubpb.PubsubMessage{Data: []byte(fmt.Sprintf("%d", j))}
			ms = append(ms, &m)
		}
		req := &pubsubpb.PublishRequest{
			Topic:    "projects/gocloud-212523/topics/hits",
			Messages: ms,
		}
		_, err := client.Publish(ctx, req)
		if err != nil {
			log.Fatal(err)
		}

		if *showingSendRates {
			sendsMu.Lock()
			sendsPerSec += batchSize
			log.Printf("sends/sec: %d", sendsPerSec)
			sendsMu.Unlock()
			time.AfterFunc(time.Second, func() {
				sendsMu.Lock()
				sendsPerSec -= batchSize
				log.Printf("sends/sec: %d", sendsPerSec)
				sendsMu.Unlock()
			})
		}
	}
}

// clock runs the given function and returns how long it took to run.
func clock(f func()) time.Duration {
	t0 := time.Now()
	f()
	return time.Now().Sub(t0)
}
