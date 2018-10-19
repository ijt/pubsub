// bench.go
package main

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	pubsublow "cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

const (
	subscriptionName = "projects/gocloud-212523/subscriptions/hits1"
	projectID        = "gocloud-212523"
)

func main() {
	ctx := context.Background()
	go send(ctx)
	benchmarkReceive(ctx)
	benchmarkBatchReceive(ctx)
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
	log.Printf("Benchmarking batch receive")
	client, err := pubsublow.NewSubscriberClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgCount := 0
	ng := 10
	bench := func(b *testing.B) {
		msgCount = 0
		for i := 0; i < b.N; i++ {
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
					msgCount += len(resp.ReceivedMessages)

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
		}
	}
	r := testing.Benchmark(bench)
	avgMsgsPerNs := float32(ng) * float32(msgCount) / float32(r.T)
	avgMsgsPerSec := 1e9 * avgMsgsPerNs
	fmt.Printf("Received %.2g msgs / sec\n", avgMsgsPerSec)
}

func benchmarkReceive(ctx context.Context) {
	log.Printf("Benchmarking receive")
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}
	sub := client.Subscription("hits1")
	var mu sync.Mutex
	msgCount := 0
	bench := func(b *testing.B) {
		log.Printf("calling sub.Receive")
		cctx, cancel := context.WithCancel(ctx)
		err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
			defer msg.Ack()
			mu.Lock()
			defer mu.Unlock()
			msgCount++
			if msgCount == b.N {
				cancel()
			}
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	r := testing.Benchmark(bench)
	msgsPerNs := float32(msgCount) / float32(r.T)
	msgsPerSec := 1e9 * msgsPerNs
	fmt.Printf("Received %.2g msgs / sec\n", msgsPerSec)
}

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
	}
}
