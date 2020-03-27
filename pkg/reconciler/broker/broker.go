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

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	brokerlisters "github.com/google/knative-gcp/pkg/client/listers/broker/v1beta1"
	gpubsub "github.com/google/knative-gcp/pkg/gclient/pubsub"
	"github.com/google/knative-gcp/pkg/reconciler"
	"github.com/google/knative-gcp/pkg/utils"
	"go.uber.org/zap"
	gstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
)

const (
	// Name of the corev1.Events emitted from the Broker reconciliation process.
	brokerReconcileError = "BrokerReconcileError"
	brokerReconciled     = "BrokerReconciled"
	topicCreated         = "TopicCreated"
	subCreated           = "SubscriptionCreated"
	topicDeleted         = "TopicDeleted"
	subDeleted           = "SubscriptionDeleted"
)

// TODO mostly deal with gcp resources here. defer other things like deployments while
// data plane strategy is worked out.
// can use PubSubBase to create Topic/PS for broker and Topic/PS for Trigger.
// Each will get relevant conditions in their status.
// This will create
// PubSubSpec is slightly more complicated because of the config object indirection. Need to build the
// PubSubSpec object on the fly from broker config.

type Reconciler struct {
	*reconciler.Base

	// listers index properties about resources
	brokerLister    brokerlisters.BrokerLister
	endpointsLister corev1listers.EndpointsLister
	triggerLister   brokerlisters.TriggerLister

	// Dynamic tracker to track KResources. It tracks the dependency between Triggers and Sources.
	kresourceTracker duck.ListableTracker

	// Dynamic tracker to track AddressableTypes. It tracks Trigger subscribers.
	addressableTracker duck.ListableTracker
	uriResolver        *resolver.URIResolver

	// CreateClientFn is the function used to create the Pub/Sub client that interacts with Pub/Sub.
	// This is needed so that we can inject a mock client for UTs purposes.
	CreateClientFn gpubsub.CreateFn

	// If specified, only reconcile brokers with these labels
	//TODO(grantr): seems to be unused in MT broker?
	brokerClass string
}

// Check that our Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)
var _ brokerreconciler.Finalizer = (*Reconciler)(nil)

var brokerGVK = brokerv1beta1.SchemeGroupVersion.WithKind("Broker")

// ReconcilerArgs are the arguments needed to create a broker.Reconciler.
type ReconcilerArgs struct {
	//TODO
}

func newReconciledNormal(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerReconciled, "Broker reconciled: \"%s/%s\"", namespace, name)
}

func (r *Reconciler) ReconcileKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	err := r.reconcileBroker(ctx, b)
	if err != nil {
		logging.FromContext(ctx).Error("Problem reconciling broker", zap.Error(err))
	}

	//TODO instead of returning on error, update the data plane configmap with
	// whatever info is available. This should all be in the same configmap
	// so it's transactional.

	if false { //TODO b.Status.IsReady() {
		// So, at this point the Broker is ready and everything should be solid
		// for the triggers to act upon, so reconcile them.
		te := r.reconcileTriggers(ctx, b)
		if te != nil {
			logging.FromContext(ctx).Error("Problem reconciling triggers", zap.Error(te))
			return fmt.Errorf("failed to reconcile triggers: %v", te)
		}
	} else {
		// Broker is not ready, but propagate its status to triggers.
		if te := r.propagateBrokerStatusToTriggers(ctx, b.Namespace, b.Name, &b.Status); te != nil {
			return fmt.Errorf("Trigger reconcile failed: %v", te)
		}
	}
	return err
}

func (r *Reconciler) FinalizeKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx).Desugar()

	//HACKHACKHACK
	b.Status.TopicID = "TODOchildname"
	b.Status.SubscriptionID = "TODOchildname"

	// Delete topic. Pull subscriptions continue pulling from the topic until deleted themselves.
	if b.Status.TopicID != "" {
		// At this point the project ID should have been populated in the status.
		// Querying Pub/Sub as the topic could have been deleted outside the cluster (e.g, through gcloud).
		client, err := r.CreateClientFn(ctx, b.Status.ProjectID)
		if err != nil {
			logger.Error("Failed to create Pub/Sub client", zap.Error(err))
			return err
		}
		defer client.Close()

		t := client.Topic(b.Status.TopicID)
		exists, err := t.Exists(ctx)
		if err != nil {
			logger.Error("Failed to verify Pub/Sub topic exists", zap.Error(err))
			return err
		}
		if exists {
			if err := t.Delete(ctx); err != nil {
				logger.Error("Failed to delete Pub/Sub topic", zap.Error(err))
				return err
			}
			logger.Info("Deleted PubSub topic", zap.String("name", t.ID()))
			r.Recorder.Eventf(b, corev1.EventTypeNormal, topicDeleted, "Deleted PubSub topic %q", t.ID())
		}
	}

	// Delete pull subscription.
	// TODO could alternately set expiration policy to make pubsub delete it after some idle time.
	// https://cloud.google.com/pubsub/docs/admin#deleting_a_topic
	if b.Status.SubscriptionID != "" {
		// At this point the project ID should have been populated in the status.
		// Querying Pub/Sub as the subscription could have been deleted outside the cluster (e.g, through gcloud).
		client, err := r.CreateClientFn(ctx, b.Status.ProjectID)
		if err != nil {
			logger.Error("Failed to create Pub/Sub client", zap.Error(err))
			return err
		}
		defer client.Close()

		sub := client.Subscription(b.Status.SubscriptionID)
		exists, err := sub.Exists(ctx)
		if err != nil {
			logger.Error("Failed to verify Pub/Sub subscription exists", zap.Error(err))
			return err
		}
		if exists {
			if err := sub.Delete(ctx); err != nil {
				logger.Error("Failed to delete Pub/Sub subscription", zap.Error(err))
				return err
			}
			logger.Info("Deleted PubSub subscription", zap.String("name", sub.ID()))
			r.Recorder.Eventf(b, corev1.EventTypeNormal, subDeleted, "Deleted PubSub subscription %q", sub.ID())
		}
	}

	// Update triggers to have a missing broker and be unready.
	if err := r.propagateBrokerStatusToTriggers(ctx, b.Namespace, b.Name, nil); err != nil {
		return fmt.Errorf("Trigger reconcile failed: %v", err)
	}
	return newReconciledNormal(b.Namespace, b.Name)
}

func (r *Reconciler) reconcileBroker(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Reconciling", zap.Any("Broker", b))
	//TODO b.Status.InitializeConditions()
	b.Status.ObservedGeneration = b.Generation

	// Create decoupling topic and pullsub for this broker. Ingress will push
	// to this topic and fanout will pull.
	// NOTE(grantr): This code can co-exist with a shared decoupling topic.
	// If we use a shared one, this topic and its sub will sit idle with zero cost.
	if err := r.reconcileDecouplingTopicAndSub(ctx, b); err != nil {
		return fmt.Errorf("Decoupling topic reconcile failed: %w", err)
	}

	return nil
}

func (r *Reconciler) reconcileDecouplingTopicAndSub(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Reconciling decoupling topic", zap.Any("Broker", b))
	// set ProjectID from config or metadata
	if b.Status.ProjectID == "" {
		//TODO support passing in project
		projectID, err := utils.ProjectID("")
		if err != nil {
			logger.Error("Failed to find project id", zap.Error(err))
			return err
		}
		// Set the projectID in the status.
		b.Status.ProjectID = projectID
	}

	// Auth to GCP is handled by having the GOOGLE_APPLICATION_CREDENTIALS environment variable
	// pointing at a credential file.
	client, err := r.CreateClientFn(ctx, b.Status.ProjectID)
	if err != nil {
		logger.Error("Failed to create Pub/Sub client", zap.Error(err))
		return err
	}
	defer client.Close()

	// Check if topic exists, and if not, create it.
	topicID := b.Status.TopicID
	if topicID == "" {
		topicID = "TODOchildname"
	}

	t := client.Topic(topicID)
	exists, err := t.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub topic exists", zap.Error(err))
		return err
	}

	if !exists {
		// Create a new topic.
		t, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			// For some reason (maybe some cache invalidation thing), sometimes t.Exists returns that the topic
			// doesn't exist but it actually does. When we try to create it again, it fails with an AlreadyExists
			// reason. We check for that error here. If it happens, then return nil.
			if _, ok := gstatus.FromError(err); !ok {
				logger.Error("Failed from Pub/Sub client while creating topic", zap.Error(err))
				return err
			}
			logger.Error("Failed to create Pub/Sub topic", zap.Error(err))
			return err
		}
		logger.Info("Created PubSub topic", zap.String("name", t.ID()))
		r.Recorder.Eventf(b, corev1.EventTypeNormal, topicCreated, "Created PubSub topic %q", t.ID())
	}

	b.Status.TopicID = t.ID()

	// Check if PullSub exists, and if not, create it.
	subID := b.Status.SubscriptionID
	if subID == "" {
		subID = "TODOchildname"
	}

	sub := client.Subscription(subID)
	subExists, err := sub.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub subscription exists", zap.Error(err))
		return err
	}

	if !subExists {
		subConfig := gpubsub.SubscriptionConfig{
			Topic: t,
			//TODO(grantr): configure these settings?
			// AckDeadline
			// RetentionDuration
		}
		// Create a new subscription to the previous topic with the given name.
		sub, err = client.CreateSubscription(ctx, subID, subConfig)
		if err != nil {
			logger.Error("Failed to create subscription", zap.Error(err))
			return err
		}
		logger.Info("Created PubSub subscription", zap.String("name", sub.ID()))
		r.Recorder.Eventf(b, corev1.EventTypeNormal, subCreated, "Created PubSub subscription %q", sub.ID())
	}
	//TODO update the subscription's config if needed.
	b.Status.SubscriptionID = sub.ID()

	return nil
}

// reconcileTriggers reconciles the Triggers that are pointed to this broker
func (r *Reconciler) reconcileTriggers(ctx context.Context, b *brokerv1beta1.Broker) error {

	// TODO: Figure out the labels stuff... If webhook does it, we can filter like this...
	// Find all the Triggers that have been labeled as belonging to me
	/*
		triggers, err := r.triggerLister.Triggers(b.Namespace).List(labels.SelectorFromSet(brokerLabels(b.brokerClass)))
	*/
	triggers, err := r.triggerLister.Triggers(b.Namespace).List(labels.Everything())
	if err != nil {
		return err
	}
	for _, t := range triggers {
		if t.Spec.Broker == b.Name {
			trigger := t.DeepCopy()
			tErr := r.reconcileTrigger(ctx, b, trigger)
			if tErr != nil {
				r.Logger.Error("Reconciling trigger failed:", zap.String("name", t.Name), zap.Error(err))
				r.Recorder.Eventf(trigger, corev1.EventTypeWarning, triggerReconcileFailed, "Trigger reconcile failed: %v", tErr)
			} else {
				r.Recorder.Event(trigger, corev1.EventTypeNormal, triggerReconciled, "Trigger reconciled")
			}
			trigger.Status.ObservedGeneration = t.Generation
			if _, updateStatusErr := r.updateTriggerStatus(ctx, trigger); updateStatusErr != nil {
				logging.FromContext(ctx).Error("Failed to update Trigger status", zap.Error(updateStatusErr))
				r.Recorder.Eventf(trigger, corev1.EventTypeWarning, triggerUpdateStatusFailed, "Failed to update Trigger's status: %v", updateStatusErr)
			}
		}
	}

	return nil
}

func (r *Reconciler) propagateBrokerStatusToTriggers(ctx context.Context, namespace, name string, bs *brokerv1beta1.BrokerStatus) error {
	triggers, err := r.triggerLister.Triggers(namespace).List(labels.Everything())
	if err != nil {
		return err
	}
	for _, t := range triggers {
		if t.Spec.Broker == name {
			// Don't modify informers copy
			trigger := t.DeepCopy()
			trigger.Status.InitializeConditions()
			if bs == nil {
				trigger.Status.MarkBrokerFailed("BrokerDoesNotExist", "Broker %q does not exist", name)
			} else {
				//TODO types
				//trigger.Status.PropagateBrokerStatus(bs.BrokerStatus)
			}
			if _, updateStatusErr := r.updateTriggerStatus(ctx, trigger); updateStatusErr != nil {
				logging.FromContext(ctx).Error("Failed to update Trigger status", zap.Error(updateStatusErr))
				r.Recorder.Eventf(trigger, corev1.EventTypeWarning, triggerUpdateStatusFailed, "Failed to update Trigger's status: %v", updateStatusErr)
				return updateStatusErr
			}
		}
	}
	return nil
}
