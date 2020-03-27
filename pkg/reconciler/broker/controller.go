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

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	brokerinformer "github.com/google/knative-gcp/pkg/client/injection/informers/broker/v1beta1/broker"
	triggerinformer "github.com/google/knative-gcp/pkg/client/injection/informers/broker/v1beta1/trigger"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	gpubsub "github.com/google/knative-gcp/pkg/gclient/pubsub"
	"github.com/google/knative-gcp/pkg/reconciler"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/apis/eventing"
	eventingv1beta1 "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/client/injection/ducks/duck/v1/conditions"
	configmapinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/configmap"
	endpointsinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/endpoints"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
)

const (
	// ReconcilerName is the name of the reconciler
	ReconcilerName = "GCPBroker"
	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "gcp-broker-controller"
)

func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	configMapInformer := configmapinformer.Get(ctx)
	endpointsInformer := endpointsinformer.Get(ctx)

	r := &Reconciler{
		Base:            reconciler.NewBase(ctx, controllerAgentName, cmw),
		brokerLister:    brokerInformer.Lister(),
		triggerLister:   triggerInformer.Lister(),
		configMapLister: configmapInformer.Lister(),
		endpointsLister: endpointsInformer.Lister(),
		CreateClientFn:  gpubsub.NewClient,
		brokerClass:     brokerv1beta1.BrokerClass,
	}
	impl := brokerreconciler.NewImpl(ctx, r)

	r.Logger.Info("Setting up event handlers")

	r.kresourceTracker = duck.NewListableTracker(ctx, conditions.Get, impl.EnqueueKey, controller.GetTrackerLease(ctx))
	r.addressableTracker = duck.NewListableTracker(ctx, addressable.Get, impl.EnqueueKey, controller.GetTrackerLease(ctx))
	r.uriResolver = resolver.NewURIResolver(ctx, impl.EnqueueKey)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.AnnotationFilterFunc(eventingv1beta1.BrokerClassAnnotationKey, brokerv1beta1.BrokerClass, false /*allowUnset*/),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	configMapInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.LabelExistsFilterFunc(eventing.BrokerLabelKey),
		Handler:    controller.HandleAll(impl.EnqueueLabelOfNamespaceScopedResource("" /*any namespace*/, eventing.BrokerLabelKey)),
	})

	//TODO https://github.com/knative/eventing/pull/2779/files
	//TODO this won't work if there's a shared service
	endpointsInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.LabelExistsFilterFunc(eventing.BrokerLabelKey),
		Handler:    controller.HandleAll(impl.EnqueueLabelOfNamespaceScopedResource("" /*any namespace*/, eventing.BrokerLabelKey)),
	})

	triggerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if trigger, ok := obj.(*brokerv1beta1.Trigger); ok {
				impl.EnqueueKey(types.NamespacedName{Namespace: trigger.Namespace, Name: trigger.Spec.Broker})
			}
		},
	))

	return impl
}
