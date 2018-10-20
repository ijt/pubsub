// bench.go
package main

import (
	"flag"
	"fmt"
	"log"
	"testing"

	// Low-level pubsub API
	pubsublow "cloud.google.com/go/pubsub/apiv1"
	"golang.org/x/net/context"
	pubsubpb "google.golang.org/genproto/googleapis/pubsub/v1"
)

const projectID = "gocloud-212523"

var fullTopic = flag.String("fulltopic", "projects/gocloud-212523/topics/hits", "full topic path")
var batchSize = flag.Int("batchsize", 1000, "how many messages to send in each batch")
var showingSendRates = flag.Bool("sendrates", false, "whether to show how many messages per second are being sent")

func main() {
	flag.Parse()
	ctx := context.Background()
	benchmarkBatchSend(ctx)
}

func benchmarkBatchSend(ctx context.Context) {
	fmt.Printf("\nBenchmarking batch send\n")
	client, err := pubsublow.NewPublisherClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgCount := 0
	bench := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var ms []*pubsubpb.PubsubMessage
			for j := 0; j < *batchSize; j++ {
				m := pubsubpb.PubsubMessage{Data: []byte(fmt.Sprintf("%d", j))}
				ms = append(ms, &m)
			}
			req := &pubsubpb.PublishRequest{
				Topic:    *fullTopic,
				Messages: ms,
			}
			_, err := client.Publish(ctx, req)
			if err != nil {
				log.Fatal(err)
			}
			msgCount += len(ms)
		}
	}
	r := testing.Benchmark(bench)
	msgsPerNs := float32(msgCount) / float32(r.T)
	msgsPerSec := 1e9 * msgsPerNs
	fmt.Printf("%8.2g msgs/sec\n", msgsPerSec)
}
