/*
Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ingress

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/knative-gcp/cmd/broker/ingress/lib"
	"github.com/google/knative-gcp/pkg/broker/config"
)

func TestIngressHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := setUp(t, broker1)

	go lib.StartIngress(ctx, runner.env)

	event := createTestEvent("test-id")
	if !waitIngressReady(runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)], 10*time.Second) {
		t.Fatal("Timed out waiting for ingress to become ready.")
	}
	res := runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)].Send(ctx, *event)
	if !cloudevents.IsACK(res) {
		t.Errorf("Error from sending events to the ingress: %v", res)
	}

	// TODO call teardown

}

func TestStressIngressHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := setUp(t, broker1)
	defer runner.tearDown()

	go lib.StartIngress(ctx, runner.env)

	if !waitIngressReady(runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)], 10*time.Second) {
		t.Fatal("Timed out waiting for ingress to become ready.")
	}

	tests := []struct {
		name       string
		numEvents  int
		numClients int
	}{
		{
			numEvents:  1000,
			numClients: 1,
		},
		{
			numEvents:  1000,
			numClients: 1,
		},
		{
			numEvents:  1000,
			numClients: 2,
		},
		{
			numEvents:  1000,
			numClients: 5,
		},
		{
			numEvents:  1000,
			numClients: 10,
		},
		{
			numEvents:  1000,
			numClients: 20,
		},
		{
			numEvents:  1000,
			numClients: 50,
		},
		{
			numEvents:  1000,
			numClients: 100,
		},
		{
			numEvents:  1000,
			numClients: 200,
		},
		{
			numEvents:  1000,
			numClients: 500,
		},
	}

	var total int
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total = total + tt.numEvents*tt.numClients
			start := time.Now()
			wg := sync.WaitGroup{}
			wg.Add(tt.numClients)
			for i := 0; i < tt.numClients; i++ {
				go func(i int) {
					sendEvent(ctx, t, runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)], strconv.Itoa(i), tt.numEvents)
					wg.Done()
				}(i)
			}
			wg.Wait()
			duration := time.Now().Sub(start)
			throughput := float64(tt.numEvents*tt.numClients) / duration.Seconds()
			latency := duration.Milliseconds() / int64(tt.numEvents)
			fmt.Printf("\n Duration: %v, num events: %v, throughput: %v\n", duration.Seconds(), tt.numEvents*tt.numClients, throughput)
			fmt.Printf("\n Num clients: %v, num events per client: %v, avg latency: %v, throughput: %v\n", tt.numClients, tt.numEvents, latency, throughput)
		})
	}

	success := pullEvent(t, runner.subs[config.BrokerKey(broker1.Namespace, broker1.Name)], int32(total))
	fmt.Printf("\n Success: %v\n", success)

	// TODO call teardown

}

func pullEvent(tb testing.TB, sub *pubsub.Subscription, limit int32) int32 {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var success int32
	fmt.Println("Pulling events with limit: ", limit)
	count := make(chan int32)
	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			m.Ack()
			atomic.AddInt32(&success, 1)
			if limit <= atomic.LoadInt32(&success) {
				fmt.Println("Calling cancel because success=", atomic.LoadInt32(&success))
				count <- atomic.LoadInt32(&success)
				cancel()
			}
		})
		if err != nil {
			fmt.Println("Calling cancel because ERROR", err)
			cancel()
			count <- atomic.LoadInt32(&success)
		}
	}()
	return <-count
}

func sendEvent(ctx context.Context, tb testing.TB, client cloudevents.Client, key string, num int) {
	for i := 0; i < num; i++ {
		event := cloudevents.NewEvent()
		event.SetID("test-id-" + key + "-" + strconv.Itoa(i))
		event.SetSource("test-source")
		event.SetType("test-type")
		res := client.Send(ctx, event)
		if !cloudevents.IsACK(res) {
			tb.Errorf("Error from sending events to the ingress: %v", res)
		}
	}
}
