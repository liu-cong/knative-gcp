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

package scheduler

import (
	"context"

	"github.com/google/knative-gcp/pkg/apis/events/v1alpha1"
	"github.com/google/knative-gcp/pkg/reconciler"
	"github.com/google/knative-gcp/pkg/reconciler/pubsub"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"

	cloudschedulersourceinformers "github.com/google/knative-gcp/pkg/client/injection/informers/events/v1alpha1/cloudschedulersource"
	pullsubscriptioninformers "github.com/google/knative-gcp/pkg/client/injection/informers/pubsub/v1alpha1/pullsubscription"
	topicinformers "github.com/google/knative-gcp/pkg/client/injection/informers/pubsub/v1alpha1/topic"
	cloudschedulersourcereconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/events/v1alpha1/cloudschedulersource"
	gscheduler "github.com/google/knative-gcp/pkg/gclient/scheduler"
)

const (
	// reconcilerName is the name of the reconciler
	reconcilerName = "CloudSchedulerSource"

	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "cloud-run-events-scheduler-source-controller"

	// receiveAdapterName is the string used as name for the receive adapter pod.
	receiveAdapterName = "cloudschedulersource.events.cloud.google.com"
)

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {

	pullsubscriptionInformer := pullsubscriptioninformers.Get(ctx)
	topicInformer := topicinformers.Get(ctx)
	cloudschedulersourceInformer := cloudschedulersourceinformers.Get(ctx)

	c := &Reconciler{
		PubSubBase:      pubsub.NewPubSubBase(ctx, controllerAgentName, receiveAdapterName, cmw),
		schedulerLister: cloudschedulersourceInformer.Lister(),
		createClientFn:  gscheduler.NewClient,
	}
	impl := cloudschedulersourcereconciler.NewImpl(ctx, c)

	c.Logger.Info("Setting up event handlers")
	cloudschedulersourceInformer.Informer().AddEventHandlerWithResyncPeriod(controller.HandleAll(impl.Enqueue), reconciler.DefaultResyncPeriod)

	topicInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.Filter(v1alpha1.SchemeGroupVersion.WithKind("CloudSchedulerSource")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	pullsubscriptionInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.Filter(v1alpha1.SchemeGroupVersion.WithKind("CloudSchedulerSource")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
