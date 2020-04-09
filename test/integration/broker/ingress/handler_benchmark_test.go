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
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/knative-gcp/cmd/broker/ingress/lib"
	"github.com/google/knative-gcp/pkg/broker/config"
)

func BenchmarkIngressHandler(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := setUp(b, broker1)

	go lib.StartIngress(ctx, runner.env)

	event := createTestEvent("test-id")
	if !waitIngressReady(runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)], 10*time.Second) {
		b.Fatal("Timed out waiting for ingress to become ready.")
	}

	fmt.Println("======Start benchmarking....")
	b.Run("", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res := runner.clients[config.BrokerKey(broker1.Namespace, broker1.Name)].Send(ctx, *event)
			if !cloudevents.IsACK(res) {
				b.Errorf("Error from sending events to the ingress: %v", res)
			}
		}
	})

	// TODO call teardown

}
