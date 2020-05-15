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
	"github.com/google/knative-gcp/pkg/broker/config/memory"
	injectionclient "github.com/google/knative-gcp/pkg/client/injection/client"
	"github.com/google/knative-gcp/pkg/client/injection/ducks/duck/v1alpha1/resource"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	triggerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/trigger"
	"github.com/google/knative-gcp/pkg/reconciler"
	. "github.com/google/knative-gcp/pkg/reconciler/testing"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
	triggerName = "test-trigger"

	triggerFinalizerName = "triggers.eventing.knative.dev"

	subscriberURI     = "http://example.com/subscriber/"
	subscriberKind    = "Service"
	subscriberName    = "subscriber-name"
	subscriberGroup   = "serving.knative.dev"
	subscriberVersion = "v1"
)

var (
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
	table := TableTest{{
		Name: "bad workqueue key",
		Key:  "too/many/parts",
	}, {
		Name: "key not found",
		Key:  testKey,
	}, {
		Name: "Trigger is being deleted, no topic or sub exists, broker ready",
		Key:  testKey,
		Objects: []runtime.Object{
			NewBroker(brokerName, testNS,
				WithBrokerClass(brokerv1beta1.BrokerClass),
				WithInitBrokerConditions,
				WithBrokerUID(testUID),
				WithBrokerReadyURI(brokerAddress),
				WithBrokerFinalizers(brokerFinalizerName)),
			NewEndpoints(ingressServiceName, systemNS,
				WithEndpointsAddresses(corev1.EndpointAddress{IP: "127.0.0.1"})),
			NewTrigger(triggerName, testNS, brokerName,
				WithTriggerUID(testUID),
				WithTriggerDeletionTimestamp,
				WithTriggerFinalizers(triggerFinalizerName)),
		},
		WantEvents: []string{
			triggerFinalizerUpdatedEvent,
			triggerFinalizedEvent,
			brokerReconciledEvent,
		},
		WantPatches: []clientgotesting.PatchActionImpl{
			patchRemoveFinalizers(testNS, triggerName),
		},
		OtherTestData: map[string]interface{}{
			"pre": []PubsubAction{
				TopicAndSub("cre-bkr_testnamespace_test-broker_abc123"),
			},
		},
		PostConditions: []func(*testing.T, *TableRow){
			OnlyTopics("cre-bkr_testnamespace_test-broker_abc123"),
			OnlySubscriptions("cre-bkr_testnamespace_test-broker_abc123"),
		},
	}, {
		Name: "Trigger is being deleted, topic and sub exists, broker ready",
		Key:  testKey,
		Objects: []runtime.Object{
			NewBroker(brokerName, testNS,
				WithBrokerClass(brokerv1beta1.BrokerClass),
				WithInitBrokerConditions,
				WithBrokerUID(testUID),
				WithBrokerReadyURI(brokerAddress),
				WithBrokerFinalizers(brokerFinalizerName)),
			NewEndpoints(ingressServiceName, systemNS,
				WithEndpointsAddresses(corev1.EndpointAddress{IP: "127.0.0.1"})),
			NewTrigger(triggerName, testNS, brokerName,
				WithTriggerUID(testUID),
				WithTriggerDeletionTimestamp,
				WithTriggerFinalizers(triggerFinalizerName)),
		},
		WantEvents: []string{
			Eventf(corev1.EventTypeNormal, "TopicDeleted", `Deleted PubSub topic "cre-tgr_testnamespace_test-trigger_abc123"`),
			Eventf(corev1.EventTypeNormal, "SubscriptionDeleted", `Deleted PubSub subscription "cre-tgr_testnamespace_test-trigger_abc123"`),
			triggerFinalizerUpdatedEvent,
			triggerFinalizedEvent,
			brokerReconciledEvent,
		},
		WantPatches: []clientgotesting.PatchActionImpl{
			patchRemoveFinalizers(testNS, triggerName),
		},
		OtherTestData: map[string]interface{}{
			"pre": []PubsubAction{
				TopicAndSub("cre-bkr_testnamespace_test-broker_abc123"),
				TopicAndSub("cre-tgr_testnamespace_test-trigger_abc123"),
			},
		},
		PostConditions: []func(*testing.T, *TableRow){
			OnlyTopics("cre-bkr_testnamespace_test-broker_abc123"),
			OnlySubscriptions("cre-bkr_testnamespace_test-broker_abc123"),
		},
	}, {
		Name: "Trigger created, broker ready, subscriber is addressable",
		Key:  testKey,
		Objects: []runtime.Object{
			NewBroker(brokerName, testNS,
				WithBrokerClass(brokerv1beta1.BrokerClass),
				WithInitBrokerConditions,
				WithBrokerUID(testUID),
				WithBrokerReadyURI(brokerAddress),
				WithBrokerFinalizers(brokerFinalizerName)),
			NewEndpoints(ingressServiceName, systemNS,
				WithEndpointsAddresses(corev1.EndpointAddress{IP: "127.0.0.1"})),
			makeSubscriberAddressableAsUnstructured(),
			NewTrigger(triggerName, testNS, brokerName,
				WithTriggerUID(testUID),
				WithTriggerSubscriberRef(subscriberGVK, subscriberName, testNS)),
		},
		WantStatusUpdates: []clientgotesting.UpdateActionImpl{{
			Object: NewTrigger(triggerName, testNS, brokerName,
				WithTriggerUID(testUID),
				WithTriggerSubscriberRef(subscriberGVK, subscriberName, testNS),
				WithTriggerBrokerReady,
				WithTriggerSubscriptionReady,
				WithTriggerTopicReady,
				WithTriggerDependencyReady,
				WithTriggerSubscriberResolvedSucceeded,
				WithTriggerStatusSubscriberURI(subscriberURI),
			),
		}},
		WantEvents: []string{
			triggerFinalizerUpdatedEvent,
			Eventf(corev1.EventTypeNormal, "TopicCreated", `Created PubSub topic "cre-tgr_testnamespace_test-trigger_abc123"`),
			Eventf(corev1.EventTypeNormal, "SubscriptionCreated", `Created PubSub subscription "cre-tgr_testnamespace_test-trigger_abc123"`),
			triggerReconciledEvent,
			brokerReconciledEvent,
		},
		WantPatches: []clientgotesting.PatchActionImpl{
			patchFinalizers(testNS, triggerName, triggerFinalizerName),
		},
		OtherTestData: map[string]interface{}{
			"pre": []PubsubAction{
				TopicAndSub("cre-bkr_testnamespace_test-broker_abc123"),
			},
		},
		PostConditions: []func(*testing.T, *TableRow){
			OnlyTopics("cre-bkr_testnamespace_test-broker_abc123", "cre-tgr_testnamespace_test-trigger_abc123"),
			OnlySubscriptions("cre-bkr_testnamespace_test-broker_abc123", "cre-tgr_testnamespace_test-trigger_abc123"),
		},
	}}

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
			triggerLister:      listers.GetTriggerLister(),
			configMapLister:    listers.GetConfigMapLister(),
			endpointsLister:    listers.GetEndpointsLister(),
			targetsConfig:      memory.NewEmptyTargets(),
			targetsNeedsUpdate: make(chan struct{}),
			projectID:          testProject,
			pubsubClient:       psclient,
		}

		tr := &TriggerReconciler{
			Base:         reconciler.NewBase(ctx, controllerAgentName, cmw),
			projectID:    testProject,
			pubsubClient: psclient,
		}

		tr.kresourceTracker = duck.NewListableTracker(ctx, conditions.Get, func(types.NamespacedName) {}, 0)
		tr.addressableTracker = duck.NewListableTracker(ctx, addressable.Get, func(types.NamespacedName) {}, 0)
		tr.uriResolver = resolver.NewURIResolver(ctx, func(types.NamespacedName) {})

		r.triggerGenReconciler = triggerreconciler.NewReconciler(
			ctx,
			r.Logger,
			injectionclient.Get(ctx),
			r.triggerLister,
			r.Recorder,
			tr,
		)

		return brokerreconciler.NewReconciler(ctx, r.Logger, r.RunClientSet, listers.GetBrokerLister(), r.Recorder, r, brokerv1beta1.BrokerClass)
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
