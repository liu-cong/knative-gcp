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

package broker

import (
	"context"
	"fmt"
	"testing"

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	"github.com/google/knative-gcp/pkg/client/injection/ducks/duck/v1alpha1/resource"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	gpubsub "github.com/google/knative-gcp/pkg/gclient/pubsub/testing"
	"github.com/google/knative-gcp/pkg/reconciler"
	. "github.com/google/knative-gcp/pkg/reconciler/testing"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
)

const (
	testNS     = "testnamespace"
	brokerName = "test-broker"

	testProject = "test-project-id"
	generation  = 1
)

var (
	testKey = fmt.Sprintf("%s/%s", testNS, brokerName)
)

func init() {
	// Add types to scheme
	_ = brokerv1beta1.AddToScheme(scheme.Scheme)
}

func TestAllCases(t *testing.T) {
	table := TableTest{{
		Name: "bad workqueue key",
		Key:  "too/many/parts",
	}, {
		Name: "key not found",
		Key:  testKey,
	}, {
		Name: "Broker is being deleted, no topic or sub exists",
		Key:  testKey,
		Objects: []runtime.Object{
			NewBroker(brokerName, testNS,
				WithBrokerClass(brokerv1beta1.BrokerClass),
				WithInitBrokerConditions,
				WithBrokerDeletionTimestamp),
		},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, "BrokerFinalized", `Broker finalized: "testnamespace/test-broker"`),
		},
		OtherTestData: map[string]interface{}{
			"ps": gpubsub.TestClientData{
				TopicData: gpubsub.TestTopicData{
					Exists: false,
				},
				SubscriptionData: gpubsub.TestSubscriptionData{
					Exists: false,
				},
			},
		},
	}, {
		Name: "Broker is being deleted, topic and sub exists",
		Key:  testKey,
		Objects: []runtime.Object{
			NewBroker(brokerName, testNS,
				WithBrokerClass(brokerv1beta1.BrokerClass),
				WithInitBrokerConditions,
				WithBrokerDeletionTimestamp),
		},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, "TopicDeleted", `Deleted PubSub topic "cre-bkr_testnamespace_test-broker_"`),
			Eventf(corev1.EventTypeNormal, "SubscriptionDeleted", `Deleted PubSub subscription "cre-bkr_testnamespace_test-broker_"`),
			Eventf(corev1.EventTypeNormal, "BrokerFinalized", `Broker finalized: "testnamespace/test-broker"`),
		},
		OtherTestData: map[string]interface{}{
			"ps": gpubsub.TestClientData{
				TopicData: gpubsub.TestTopicData{
					Exists: true,
				},
				SubscriptionData: gpubsub.TestSubscriptionData{
					Exists: true,
				},
			},
		},
	}}

	defer logtesting.ClearAll()
	table.Test(t, MakeFactory(func(ctx context.Context, listers *Listers, cmw configmap.Watcher, testData map[string]interface{}) controller.Reconciler {
		ctx = addressable.WithDuck(ctx)
		ctx = resource.WithDuck(ctx)
		r := &Reconciler{
			Base:               reconciler.NewBase(ctx, controllerAgentName, cmw),
			triggerLister:      listers.GetTriggerLister(),
			configMapLister:    listers.GetConfigMapLister(),
			CreateClientFn:     gpubsub.TestClientCreator(testData["ps"]),
			targetsNeedsUpdate: make(chan struct{}),
			projectID:          testProject,
		}
		return brokerreconciler.NewReconciler(ctx, r.Logger, r.RunClientSet, listers.GetBrokerLister(), r.Recorder, r, brokerv1beta1.BrokerClass)
	}))
}
