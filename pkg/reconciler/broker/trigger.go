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
	"reflect"
	"time"

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	gpubsub "github.com/google/knative-gcp/pkg/gclient/pubsub"
	"github.com/google/knative-gcp/pkg/reconciler/broker/resources"
	"github.com/google/knative-gcp/pkg/utils"
	"go.uber.org/zap"
	gstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"knative.dev/eventing/pkg/apis/eventing/v1alpha1"
	duckv1 "knative.dev/pkg/apis/duck/v1"
	"knative.dev/pkg/logging"
)

const (
	// Name of the corev1.Events emitted from the Trigger reconciliation process.
	triggerReconciled         = "TriggerReconciled"
	triggerReadinessChanged   = "TriggerReadinessChanged"
	triggerReconcileFailed    = "TriggerReconcileFailed"
	triggerUpdateStatusFailed = "TriggerUpdateStatusFailed"
)

func (r *Reconciler) reconcileTrigger(ctx context.Context, b *brokerv1beta1.Broker, t *brokerv1beta1.Trigger) error {
	logger := logging.FromContext(ctx).Desugar()
	t.Status.InitializeConditions()

	if t.DeletionTimestamp != nil {
		// TODO finalize by deleting topic/sub
		return nil
	}

	//t.Status.PropagateBrokerStatus(b.Status)

	if t.Spec.Subscriber.Ref != nil {
		// To call URIFromDestination(dest apisv1alpha1.Destination, parent interface{}), dest.Ref must have a Namespace
		// We will use the Namespace of Trigger as the Namespace of dest.Ref
		t.Spec.Subscriber.Ref.Namespace = t.GetNamespace()
	}

	subscriberURI, err := r.uriResolver.URIFromDestinationV1(t.Spec.Subscriber, b)
	if err != nil {
		logger.Error("Unable to get the Subscriber's URI", zap.Error(err))
		t.Status.MarkSubscriberResolvedFailed("Unable to get the Subscriber's URI", "%v", err)
		t.Status.SubscriberURI = nil
		return err
	}
	t.Status.SubscriberURI = subscriberURI
	t.Status.MarkSubscriberResolvedSucceeded()

	if err := r.reconcileRetryTopicAndSubscription(ctx, t); err != nil {
		return err
	}

	if err := r.checkDependencyAnnotation(ctx, t, b); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) reconcileRetryTopicAndSubscription(ctx context.Context, trig *brokerv1beta1.Trigger) error {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Reconciling retry topic")
	// get ProjectID from config or metadata
	//TODO(grantr) support configuring project in broker config
	projectID, err := utils.ProjectID("")
	if err != nil {
		logger.Error("Failed to find project id", zap.Error(err))
		return err
	}
	// Set the projectID in the status.
	trig.Status.ProjectID = projectID

	// Auth to GCP is handled by having the GOOGLE_APPLICATION_CREDENTIALS environment variable
	// pointing at a credential file.
	client, err := r.CreateClientFn(ctx, projectID)
	if err != nil {
		logger.Error("Failed to create Pub/Sub client", zap.Error(err))
		return err
	}
	defer client.Close()

	// Check if topic exists, and if not, create it.
	topicID := resources.GenerateRetryTopicName(trig)
	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub topic exists", zap.Error(err))
		return err
	}

	if !exists {
		topicConfig := &gpubsub.TopicConfig{
			Labels: map[string]string{
				"resource":  "triggers",
				"namespace": trig.Namespace,
				"name":      trig.Name,
				//TODO add resource labels, but need to be sanitized: https://cloud.google.com/pubsub/docs/labels#requirements
			},
		}
		// Create a new topic.
		logger.Debug("Creating topic with cfg", zap.String("id", topicID), zap.Any("cfg", topicConfig))
		topic, err = client.CreateTopicWithConfig(ctx, topicID, topicConfig)
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
		logger.Info("Created PubSub topic", zap.String("name", topic.ID()))
		r.Recorder.Eventf(trig, corev1.EventTypeNormal, topicCreated, "Created PubSub topic %q", topic.ID())
	}

	// TODO(grantr): this isn't actually persisted due to webhook issues.
	trig.Status.TopicID = topic.ID()

	// Check if PullSub exists, and if not, create it.
	subID := resources.GenerateRetrySubscriptionName(trig)
	sub := client.Subscription(subID)
	subExists, err := sub.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub subscription exists", zap.Error(err))
		return err
	}

	if !subExists {
		subConfig := gpubsub.SubscriptionConfig{
			Topic: topic,
			Labels: map[string]string{
				"resource":  "triggers",
				"namespace": trig.Namespace,
				"name":      trig.Name,
				//TODO add resource labels, but need to be sanitized: https://cloud.google.com/pubsub/docs/labels#requirements
			},
			//TODO(grantr): configure these settings?
			// AckDeadline
			// RetentionDuration
		}
		// Create a new subscription to the previous topic with the given name.
		logger.Debug("Creating sub with cfg", zap.String("id", subID), zap.Any("cfg", subConfig))
		sub, err = client.CreateSubscription(ctx, subID, subConfig)
		if err != nil {
			logger.Error("Failed to create subscription", zap.Error(err))
			return err
		}
		logger.Info("Created PubSub subscription", zap.String("name", sub.ID()))
		r.Recorder.Eventf(trig, corev1.EventTypeNormal, subCreated, "Created PubSub subscription %q", sub.ID())
	}
	//TODO update the subscription's config if needed.

	// TODO(grantr): this isn't actually persisted due to webhook issues.
	trig.Status.SubscriptionID = sub.ID()

	return nil
}

func (r *Reconciler) updateTriggerStatus(ctx context.Context, desired *brokerv1beta1.Trigger) (*brokerv1beta1.Trigger, error) {
	trigger, err := r.triggerLister.Triggers(desired.Namespace).Get(desired.Name)
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(trigger.Status, desired.Status) {
		return trigger, nil
	}

	becomesReady := desired.Status.IsReady() && !trigger.Status.IsReady()

	// Don't modify the informers copy.
	existing := trigger.DeepCopy()
	existing.Status = desired.Status

	trig, err := r.RunClientSet.EventingV1beta1().Triggers(desired.Namespace).UpdateStatus(existing)
	if err == nil && becomesReady {
		duration := time.Since(trig.ObjectMeta.CreationTimestamp.Time)
		r.Logger.Infof("Trigger %q became ready after %v", trigger.Name, duration)
		r.Recorder.Event(trigger, corev1.EventTypeNormal, triggerReadinessChanged, fmt.Sprintf("Trigger %q became ready", trigger.Name))
		if err := r.StatsReporter.ReportReady("Trigger", trigger.Namespace, trigger.Name, duration); err != nil {
			logging.FromContext(ctx).Desugar().Info("failed to record ready for Trigger", zap.Error(err))
		}
	}

	return trig, err
}

func (r *Reconciler) checkDependencyAnnotation(ctx context.Context, t *brokerv1beta1.Trigger, b *brokerv1beta1.Broker) error {
	if dependencyAnnotation, ok := t.GetAnnotations()[v1alpha1.DependencyAnnotation]; ok {
		dependencyObjRef, err := v1alpha1.GetObjRefFromDependencyAnnotation(dependencyAnnotation)
		if err != nil {
			t.Status.MarkDependencyFailed("ReferenceError", "Unable to unmarshal objectReference from dependency annotation of trigger: %v", err)
			return fmt.Errorf("getting object ref from dependency annotation %q: %v", dependencyAnnotation, err)
		}
		trackKResource := r.kresourceTracker.TrackInNamespace(b)
		// Trigger and its dependent source are in the same namespace, we already did the validation in the webhook.
		if err := trackKResource(dependencyObjRef); err != nil {
			return fmt.Errorf("tracking dependency: %v", err)
		}
		if err := r.propagateDependencyReadiness(ctx, t, dependencyObjRef); err != nil {
			return fmt.Errorf("propagating dependency readiness: %v", err)
		}
	} else {
		t.Status.MarkDependencySucceeded()
	}
	return nil
}

func (r *Reconciler) propagateDependencyReadiness(ctx context.Context, t *brokerv1beta1.Trigger, dependencyObjRef corev1.ObjectReference) error {
	lister, err := r.kresourceTracker.ListerFor(dependencyObjRef)
	if err != nil {
		t.Status.MarkDependencyUnknown("ListerDoesNotExist", "Failed to retrieve lister: %v", err)
		return fmt.Errorf("retrieving lister: %v", err)
	}
	dependencyObj, err := lister.ByNamespace(t.GetNamespace()).Get(dependencyObjRef.Name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			t.Status.MarkDependencyFailed("DependencyDoesNotExist", "Dependency does not exist: %v", err)
		} else {
			t.Status.MarkDependencyUnknown("DependencyGetFailed", "Failed to get dependency: %v", err)
		}
		return fmt.Errorf("getting the dependency: %v", err)
	}
	dependency := dependencyObj.(*duckv1.KResource)

	// The dependency hasn't yet reconciled our latest changes to
	// its desired state, so its conditions are outdated.
	if dependency.GetGeneration() != dependency.Status.ObservedGeneration {
		logging.FromContext(ctx).Desugar().Info("The ObjectMeta Generation of dependency is not equal to the observedGeneration of status",
			zap.Any("objectMetaGeneration", dependency.GetGeneration()),
			zap.Any("statusObservedGeneration", dependency.Status.ObservedGeneration))
		t.Status.MarkDependencyUnknown("GenerationNotEqual", "The dependency's metadata.generation, %q, is not equal to its status.observedGeneration, %q.", dependency.GetGeneration(), dependency.Status.ObservedGeneration)
		return nil
	}
	t.Status.PropagateDependencyStatus(dependency)
	return nil
}
