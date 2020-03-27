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

package resources

import (
	"fmt"

	"github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
)

// GenerateDecouplingTopicName generates a deterministic topic name for a
// Broker.
func GenerateDecouplingTopicName(b *v1beta1.Broker) string {
	return fmt.Sprintf("cre-b-%s-%s-%s", b.Namespace, b.Name, string(b.UID))
}

// GenerateDecouplingSubscriptionName generates a deterministic subscription
// name for a Broker.
func GenerateDecouplingSubscriptionName(b *v1beta1.Broker) string {
	return fmt.Sprintf("cre-b-%s-%s-%s", b.Namespace, b.Name, string(b.UID))
}

// GenerateRetryTopicName generates a deterministic topic name for a Trigger.
func GenerateRetryTopicName(t *v1beta1.Trigger) string {
	return fmt.Sprintf("cre-t-%s-%s-%s", t.Namespace, t.Name, string(t.UID))
}

// GenerateRetrySubscriptionName generates a deterministic subscription name
// for a Trigger.
func GenerateRetrySubscriptionName(t *v1beta1.Trigger) string {
	return fmt.Sprintf("cre-t-%s-%s-%s", t.Namespace, t.Name, string(t.UID))
}
