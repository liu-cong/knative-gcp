/*
Copyright 2019 Google LLC

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

package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/google/knative-gcp/pkg/gclient/iam"
)

// TopicConfig re-implements pubsub.TopicConfig to allow us to
// use a wrapped Topic internally.
type TopicConfig struct {
	// The set of labels for the topic.
	Labels map[string]string

	// The topic's message storage policy.
	MessageStoragePolicy MessageStoragePolicy

	// The name of the Cloud KMS key to be used to protect access to messages
	// published to this topic, in the format
	// "projects/P/locations/L/keyRings/R/cryptoKeys/K".
	KMSKeyName string
}

// MessageStoragePolicy re-implements pubsub.MessageStoragePolicy to allow us to
// use a wrapped Topic internally.
type MessageStoragePolicy struct {
	// AllowedPersistenceRegions is the list of GCP regions where messages that are published
	// to the topic may be persisted in storage. Messages published by publishers running in
	// non-allowed GCP regions (or running outside of GCP altogether) will be
	// routed for storage in one of the allowed regions.
	//
	// If empty, it indicates a misconfiguration at the project or organization level, which
	// will result in all Publish operations failing. This field cannot be empty in updates.
	//
	// If nil, then the policy is not defined on a topic level. When used in updates, it resets
	// the regions back to the organization level Resource Location Restriction policy.
	//
	// For more information, see
	// https://cloud.google.com/pubsub/docs/resource-location-restriction#pubsub-storage-locations.
	AllowedPersistenceRegions []string
}

// pubsubTopic wraps pubsub.Topic. Is the topic that will be used everywhere except unit tests.
type pubsubTopic struct {
	topic *pubsub.Topic
}

// Verify that it satisfies the pubsub.Topic interface.
var _ Topic = &pubsubTopic{}

// Exists implements pubsub.Topic.Exists
func (t *pubsubTopic) Exists(ctx context.Context) (bool, error) {
	return t.topic.Exists(ctx)
}

// Delete implements pubsub.Topic.Delete
func (t *pubsubTopic) Delete(ctx context.Context) error {
	return t.topic.Delete(ctx)
}

func (t *pubsubTopic) IAM() iam.Handle {
	return iam.NewIamHandle(t.topic.IAM())
}

func (t *pubsubTopic) ID() string {
	return t.topic.ID()
}
