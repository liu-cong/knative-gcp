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

package channel

import (
	"context"

	"github.com/google/knative-gcp/pkg/apis/messaging/v1alpha1"
	"github.com/google/knative-gcp/pkg/reconciler"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"

	channelinformer "github.com/google/knative-gcp/pkg/client/injection/informers/messaging/v1alpha1/channel"
	pullsubscriptioninformer "github.com/google/knative-gcp/pkg/client/injection/informers/pubsub/v1alpha1/pullsubscription"
	topicinformer "github.com/google/knative-gcp/pkg/client/injection/informers/pubsub/v1alpha1/topic"
	channelreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/messaging/v1alpha1/channel"
)

const (
	// reconcilerName is the name of the reconciler
	reconcilerName = "Channels"

	// controllerAgentName is the string used by this controller to identify
	// itself when creating events.
	controllerAgentName = "cloud-run-events-channel-controller"
)

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {

	channelInformer := channelinformer.Get(ctx)

	topicInformer := topicinformer.Get(ctx)
	pullSubscriptionInformer := pullsubscriptioninformer.Get(ctx)

	r := &Reconciler{
		Base:                   reconciler.NewBase(ctx, controllerAgentName, cmw),
		channelLister:          channelInformer.Lister(),
		topicLister:            topicInformer.Lister(),
		pullSubscriptionLister: pullSubscriptionInformer.Lister(),
	}
	impl := channelreconciler.NewImpl(ctx, r)

	r.Logger.Info("Setting up event handlers")
	channelInformer.Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	topicInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.Filter(v1alpha1.SchemeGroupVersion.WithKind("Channel")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	pullSubscriptionInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.Filter(v1alpha1.SchemeGroupVersion.WithKind("Channel")),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
