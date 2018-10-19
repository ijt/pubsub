// bench.go
package main

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func main() {
	ctx := context.Background()
	go send(ctx)
	benchmarkBatchReceive(ctx)
	benchmarkStreamingReceive(ctx)
}

func send(ctx context.Context) {
	client, err := pubsub.NewPublisherClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for g := 0; g < 100; g++ {
		go pubWorker(ctx, client, 1000)
	}
}

func benchmarkBatchReceive(ctx context.Context) {
	log.Printf("Benchmarking batch receive")
	client, err := pubsub.NewSubscriberClient(ctx)
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
						Subscription: "projects/gocloud-212523/subscriptions/hits1",
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
	avgMsgsPerNs := float32(ng) * float32(msgCount) / (float32(r.N) * float32(r.T))
	avgMsgsPerSec := 1e9 * avgMsgsPerNs
	fmt.Printf("Received %.2g msgs / sec\n", avgMsgsPerSec)
}

func benchmarkStreamingReceive(ctx context.Context) {
	log.Printf("Benchmarking streaming receive")
	var mu sync.Mutex
	client, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgCount := 0
	bench := func(b *testing.B) {
		msgCount = 0
		mu.Lock()
		pullClient, err := client.StreamingPull(ctx)
		mu.Unlock()
		if err != nil {
			log.Fatalf("failed call to StreamingPull: %v", err)
		}
		for i := 0; i < b.N; i++ {
			var ackIDs []string
			req := &pubsubpb.StreamingPullRequest{
				Subscription: "projects/gocloud-212523/subscriptions/hits1",
				AckIds:       ackIDs,
			}
			ackIDs = nil
			if err := pullClient.Send(req); err != nil {
				log.Fatalf("failed call to Send: %v", err)
			}

			mu.Lock()
			resp, err := pullClient.Recv()
			if err != nil {
				log.Fatalf("failed call to Recv: %v", err)
			}

			for _, m := range resp.ReceivedMessages {
				ackIDs = append(ackIDs, m.AckId)
			}
			ackReq := &pubsubpb.AcknowledgeRequest{
				Subscription: req.Subscription,
				AckIds:       ackIDs,
			}
			if err := client.Acknowledge(ctx, ackReq); err != nil {
				log.Fatal(err)
			}
		}
	}
	r := testing.Benchmark(bench)
	msgsPerNs := float32(msgCount) / (float32(r.N) * float32(r.T))
	msgsPerSec := 1e9 * msgsPerNs
	fmt.Printf("Received %.2g msgs / sec\n", msgsPerSec)
}

func pubWorker(ctx context.Context, client *pubsub.PublisherClient, batchSize int) {
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
