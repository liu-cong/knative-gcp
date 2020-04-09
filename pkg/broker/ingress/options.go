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
	"github.com/google/knative-gcp/pkg/broker/config/volume"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/knative-gcp/pkg/broker/config"
	"knative.dev/eventing/pkg/kncloudevents"
)

// HandlerOption is the option to configure ingress handler.
type HandlerOption func(*handler) error

// WithPort specifies the port number that ingress listens on. It will create an HttpMessageReceiver for that port.
func WithPort(port int) HandlerOption {
	return func(h *handler) error {
		h.httpReceiver = kncloudevents.NewHttpMessageReceiver(port)
		return nil
	}
}

// WithDecoupleSink specifies the decouple sink.
func WithDecoupleSink(decouple DecoupleSink) HandlerOption {
	return func(h *handler) error {
		h.decouple = decouple
		return nil
	}
}

// MultiTopicDecoupleSinkOption is the option to configure multiTopicDecoupleSink.
type MultiTopicDecoupleSinkOption func(sink *multiTopicDecoupleSink) error

// WithPubsubClient specifies the pubsub client to use.
func WithPubsubClient(client cloudevents.Client) MultiTopicDecoupleSinkOption {
	return func(sink *multiTopicDecoupleSink) error {
		sink.client = client
		return nil
	}
}

// WithProjectID creates a pubsub client for the given project ID to communicate with pubsusb decouple topics.
func WithProjectID(id string) MultiTopicDecoupleSinkOption {
	return func(sink *multiTopicDecoupleSink) error {
		ctx := context.Background()
		client, err := newPubSubClient(ctx, id)
		if err != nil {
			return fmt.Errorf("failed to create pubsub client: %v", err)
		}
		sink.client = client
		return nil
	}
}


// WithBrokerConfig specifies the broker config. It can be created by reading a configmap mount.
func WithBrokerConfig(brokerConfig config.ReadonlyTargets) MultiTopicDecoupleSinkOption {
	return func(sink *multiTopicDecoupleSink) error {
		sink.brokerConfig = brokerConfig
		return nil
	}
}

// WithBrokerConfigPath creates a brokerConfig from the given path.
func WithBrokerConfigPath(path string) MultiTopicDecoupleSinkOption {
	return func(sink *multiTopicDecoupleSink) error {
		brokerConfig, err := volume.NewTargetsFromFile(volume.WithPath(path))
		if err != nil {
			return fmt.Errorf("creating broker config for default multi topic decouple sink")
		}
		sink.brokerConfig = brokerConfig
		return nil
	}
}
