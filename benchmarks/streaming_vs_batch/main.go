// bench.go
package main

import (
	"fmt"
	"log"
	"testing"

	"cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

func main() {
	ctx := context.Background()
	go send(ctx)
	benchmarkBatchReceive(ctx)
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
	client, err := pubsub.NewSubscriberClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgCount := 0
	bench := func(b *testing.B) {
		log.Printf("bench function running, b.N = %d", b.N)
		msgCount = 0
		for i := 0; i < b.N; i++ {
			req := &pubsubpb.PullRequest{
				Subscription: "projects/gocloud-212523/subscriptions/hits1",
				MaxMessages:  1000,
			}
			resp, err := client.Pull(ctx, req)
			if err != nil {
				log.Fatal(err)
			}
			msgCount += len(resp.ReceivedMessages)
		}
	}
	r := testing.Benchmark(bench)
	avgMsgsPerNs := float32(msgCount) / (float32(r.N) * float32(r.T))
	avgMsgsPerSec := 1e9 * avgMsgsPerNs
	fmt.Printf("Received %.2g msgs / sec\n", avgMsgsPerSec)
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
