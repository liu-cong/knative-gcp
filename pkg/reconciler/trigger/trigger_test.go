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

package trigger

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"testing"

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	"github.com/google/knative-gcp/pkg/client/injection/ducks/duck/v1alpha1/resource"
	triggerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/trigger"
	"github.com/google/knative-gcp/pkg/reconciler"
	. "github.com/google/knative-gcp/pkg/reconciler/testing"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clientgotesting "k8s.io/client-go/testing"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/client/injection/ducks/duck/v1/conditions"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	logtesting "knative.dev/pkg/logging/testing"
	. "knative.dev/pkg/reconciler/testing"
	"knative.dev/pkg/resolver"
)

const (
	testNS      = "testnamespace"
	triggerName = "test-trigger"
	brokerName  = "test-broker"

	testProject = "test-project-id"

	subscriberURI     = "http://example.com/subscriber/"
	subscriberKind    = "Service"
	subscriberName    = "subscriber-name"
	subscriberGroup   = "serving.knative.dev"
	subscriberVersion = "v1"
)

var (
	testKey = fmt.Sprintf("%s/%s", testNS, triggerName)

	triggerFinalizerUpdatedEvent = Eventf(corev1.EventTypeNormal, "FinalizerUpdate", `Updated "test-trigger" finalizers`)
	triggerReconciledEvent       = Eventf(corev1.EventTypeNormal, "TriggerReconciled", `Trigger reconciled: "testnamespace/test-trigger"`)
	triggerFinalizedEvent        = Eventf(corev1.EventTypeNormal, "TriggerFinalized", `Trigger finalized: "testnamespace/test-trigger"`)

	subscriberAPIVersion = fmt.Sprintf("%s/%s", subscriberGroup, subscriberVersion)
	subscriberGVK        = metav1.GroupVersionKind{
		Group:   subscriberGroup,
		Version: subscriberVersion,
		Kind:    subscriberKind,
	}
)

func init() {
	// Add types to scheme
	_ = brokerv1beta1.AddToScheme(scheme.Scheme)
}

func TestAllCasesTrigger(t *testing.T) {
	table := TableTest{
		{
			Name: "bad workqueue key",
			Key:  "too/many/parts",
		},
		{
			Name: "key not found",
			Key:  testKey,
		},
		{
			Name: "Trigger with finalizer is being deleted, no topic or sub exists",
			Key:  testKey,
			Objects: []runtime.Object{
				NewTrigger(triggerName, testNS, brokerName,
					WithTriggerDeletionTimestamp,
					WithTriggerFinalizers(finalizerName)),
			},
			WantEvents: []string{
				triggerFinalizerUpdatedEvent,
				triggerFinalizedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchRemoveFinalizers(testNS, triggerName),
			},
		},
		{
			Name: "Trigger without finalizer is being deleted, no topic or sub exists",
			Key:  testKey,
			Objects: []runtime.Object{
				NewTrigger(triggerName, testNS, brokerName, WithTriggerDeletionTimestamp),
			},
		},
		{
			Name: "Trigger is being deleted, topic and sub exists, broker ready",
			Key:  testKey,
			Objects: []runtime.Object{
				NewTrigger(triggerName, testNS, brokerName,
					WithTriggerDeletionTimestamp,
					WithTriggerFinalizers(finalizerName)),
			},
			WantEvents: []string{
				Eventf(corev1.EventTypeNormal, "TopicDeleted", `Deleted PubSub topic "cre-tgr_testnamespace_test-trigger_abc123"`),
				Eventf(corev1.EventTypeNormal, "SubscriptionDeleted", `Deleted PubSub subscription "cre-tgr_testnamespace_test-trigger_abc123"`),
				triggerFinalizerUpdatedEvent,
				triggerFinalizedEvent,
			},
			WantPatches: []clientgotesting.PatchActionImpl{
				patchRemoveFinalizers(testNS, triggerName),
			},
			OtherTestData: map[string]interface{}{
				"pre": []PubsubAction{
					TopicAndSub("cre-tgr_testnamespace_test-trigger_abc123"),
				},
			},
		},
		//{
		//	Name: "Trigger created, broker ready, subscriber is addressable",
		//	Key:  testKey,
		//	Objects: []runtime.Object{
		//		NewBroker(brokerName, testNS,
		//			WithBrokerClass(brokerv1beta1.BrokerClass),
		//			WithInitBrokerConditions,
		//			WithBrokerUID(testUID),
		//			WithBrokerReadyURI(brokerAddress),
		//			WithBrokerFinalizers(brokerFinalizerName)),
		//		NewEndpoints(ingressServiceName, systemNS,
		//			WithEndpointsAddresses(corev1.EndpointAddress{IP: "127.0.0.1"})),
		//		makeSubscriberAddressableAsUnstructured(),
		//		NewTrigger(triggerName, testNS, brokerName,
		//			WithTriggerUID(testUID),
		//			WithTriggerSubscriberRef(subscriberGVK, subscriberName, testNS)),
		//	},
		//	WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
		//		Object: NewTrigger(triggerName, testNS, brokerName,
		//			WithTriggerUID(testUID),
		//			WithTriggerSubscriberRef(subscriberGVK, subscriberName, testNS),
		//			WithTriggerBrokerReady,
		//			WithTriggerSubscriptionReady,
		//			WithTriggerTopicReady,
		//			WithTriggerDependencyReady,
		//			WithTriggerSubscriberResolvedSucceeded,
		//			WithTriggerStatusSubscriberURI(subscriberURI),
		//		),
		//	}},
		//	WantEvents: []string{
		//		triggerFinalizerUpdatedEvent,
		//		Eventf(corev1.EventTypeNormal, "TopicCreated", `Created PubSub topic "cre-tgr_testnamespace_test-trigger_abc123"`),
		//		Eventf(corev1.EventTypeNormal, "SubscriptionCreated", `Created PubSub subscription "cre-tgr_testnamespace_test-trigger_abc123"`),
		//		triggerReconciledEvent,
		//		brokerReconciledEvent,
		//	},
		//	WantPatches: []clientgotesting.PatchActionImpl{
		//		patchFinalizers(testNS, triggerName, triggerFinalizerName),
		//	},
		//	OtherTestData: map[string]interface{}{
		//		"pre": []PubsubAction{
		//			TopicAndSub("cre-bkr_testnamespace_test-broker_abc123"),
		//		},
		//	},
		//	PostConditions: []func(*testing.T, *TableRow){
		//		OnlyTopics("cre-bkr_testnamespace_test-broker_abc123", "cre-tgr_testnamespace_test-trigger_abc123"),
		//		OnlySubscriptions("cre-bkr_testnamespace_test-broker_abc123", "cre-tgr_testnamespace_test-trigger_abc123"),
		//	},
		//},
	}

	defer logtesting.ClearAll()
	table.Test(t, MakeFactory(func(ctx context.Context, listers *Listers, cmw configmap.Watcher, testData map[string]interface{}) controller.Reconciler {
		// Insert pubsub client for PostConditions and create fixtures
		psclient, close := TestPubsubClient(ctx, testProject)
		t.Cleanup(close)
		if testData != nil {
			InjectPubsubClient(testData, psclient)
			if testData["pre"] != nil {
				fixtures := testData["pre"].([]PubsubAction)
				for _, f := range fixtures {
					f(ctx, t, psclient)
				}
			}
		}

		ctx = addressable.WithDuck(ctx)
		ctx = resource.WithDuck(ctx)
		ctx = conditions.WithDuck(ctx)

		r := &Reconciler{
			Base:               reconciler.NewBase(ctx, controllerAgentName, cmw),
			brokerLister:       listers.GetBrokerLister(),
			kresourceTracker:   duck.NewListableTracker(ctx, conditions.Get, func(types.NamespacedName) {}, 0),
			addressableTracker: duck.NewListableTracker(ctx, addressable.Get, func(types.NamespacedName) {}, 0),
			uriResolver:        resolver.NewURIResolver(ctx, func(types.NamespacedName) {}),
			projectID:          testProject,
			pubsubClient:       psclient,
		}

		return triggerreconciler.NewReconciler(ctx, r.Logger, r.RunClientSet, listers.GetTriggerLister(), r.Recorder, r, withAgentAndFinalizer(nil))
	}))
}

func makeSubscriberAddressableAsUnstructured() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": subscriberAPIVersion,
			"kind":       subscriberKind,
			"metadata": map[string]interface{}{
				"namespace": testNS,
				"name":      subscriberName,
			},
			"status": map[string]interface{}{
				"address": map[string]interface{}{
					"url": subscriberURI,
				},
			},
		},
	}
}

func patchRemoveFinalizers(namespace, name string) clientgotesting.PatchActionImpl {
	action := clientgotesting.PatchActionImpl{}
	action.Name = name
	action.Namespace = namespace
	patch := `{"metadata":{"finalizers":[],"resourceVersion":""}}`
	action.Patch = []byte(patch)
	return action
}
