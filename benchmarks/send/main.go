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

const projectID = "gocloud-212523"

var fullTopic = flag.String("fulltopic", "projects/gocloud-212523/topics/hits", "full topic path")
var batchSize = flag.Int("batchsize", 1000, "how many messages to send in each batch")
var showingSendRates = flag.Bool("sendrates", false, "whether to show how many messages per second are being sent")

func main() {
	flag.Parse()
	ctx := context.Background()
	benchmarkSend(ctx)
	benchmarkBatchSend(ctx)
}

func benchmarkBatchSend(ctx context.Context) {
	fmt.Printf("\nBenchmarking batch send\n")
	client, err := pubsublow.NewPublisherClient(ctx)
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
					mu.Lock()
					msgCount += len(ms)
					mu.Unlock()
					wg.Done()
				}()
			}
			wg.Wait()
		})
		msgsPerSec := float64(msgCount) / dt.Seconds()
		fmt.Fprintf(w, "%d\t%.2g\n", ng, msgsPerSec)
	}
	w.Flush()
}

func benchmarkSend(ctx context.Context) {
	fmt.Printf("\nBenchmarking high-level GCP PubSub API send\n")
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	t := client.Topic("hits")
	t.PublishSettings.DelayThreshold = time.Second
	t.PublishSettings.CountThreshold = 1000
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
					// Send a bunch of messages.
					var results []*pubsub.PublishResult
					for j := 0; j < *batchSize; j++ {
						r := t.Publish(ctx, &pubsub.Message{
							Data: []byte(fmt.Sprintf("%d", j)),
						})
						results = append(results, r)
					}

					// Wait for the messages to reach the server.
					var msgWg sync.WaitGroup
					msgWg.Add(len(results))
					for _, r := range results {
						go func(r *pubsub.PublishResult) {
							<-r.Ready()
							msgWg.Done()
						}(r)
					}
					msgWg.Wait()

					// Update msgCount.
					mu.Lock()
					msgCount += len(results)
					mu.Unlock()
					wg.Done()
				}()
			}
			wg.Wait()
		})
		msgsPerSec := float64(msgCount) / dt.Seconds()
		fmt.Fprintf(w, "%d\t%.2g\n", ng, msgsPerSec)
	}
	w.Flush()
}

// clock runs the given function and returns how long it took to run.
func clock(f func()) time.Duration {
	t0 := time.Now()
	f()
	return time.Now().Sub(t0)
}
