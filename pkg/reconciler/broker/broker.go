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
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	"github.com/google/knative-gcp/pkg/broker/config"
	"github.com/google/knative-gcp/pkg/broker/config/memory"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	brokerlisters "github.com/google/knative-gcp/pkg/client/listers/broker/v1beta1"
	gpubsub "github.com/google/knative-gcp/pkg/gclient/pubsub"
	"github.com/google/knative-gcp/pkg/reconciler"
	"github.com/google/knative-gcp/pkg/reconciler/broker/resources"
	"github.com/google/knative-gcp/pkg/utils"
	"go.uber.org/zap"
	gstatus "google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
)

const (
	// Name of the corev1.Events emitted from the Broker reconciliation process.
	brokerReconcileError = "BrokerReconcileError"
	brokerReconciled     = "BrokerReconciled"
	topicCreated         = "TopicCreated"
	subCreated           = "SubscriptionCreated"
	topicDeleted         = "TopicDeleted"
	subDeleted           = "SubscriptionDeleted"

	targetsCMNamespace    = "cloud-run-events"
	targetsCMName         = "broker-targets"
	targetsCMKey          = "targets"
	targetsCMResyncPeriod = 10 * time.Second
)

// TODO
// idea: assign broker resources to cell (configmap) in webhook based on a
// global configmap (in controller's namespace) of cell assignment rules, and
// label the broker with the assignment. Controller uses these labels to
// determine which configmap to create/update when a broker is reconciled, and
// to determine which brokers to reconcile when a configmap is updated.
// Initially, the assignment can be static.

// TODO bug: if broker is deleted first, triggers can't be reconciled
// TODO: verify that if topic is deleted from gcp it's recreated here

type Reconciler struct {
	*reconciler.Base

	// listers index properties about resources
	triggerLister   brokerlisters.TriggerLister
	configMapLister corev1listers.ConfigMapLister
	endpointsLister corev1listers.EndpointsLister

	// CreateClientFn is the function used to create the Pub/Sub client that interacts with Pub/Sub.
	// This is needed so that we can inject a mock client for UTs purposes.
	CreateClientFn gpubsub.CreateFn

	// Reconciles a broker's triggers
	triggerReconciler controller.Reconciler

	// TODO allow configuring multiples of these
	targetsConfig  config.Targets
	loadConfigOnce sync.Once

	// targetsNeedsUpdate is a channel that flags the targets ConfigMap as
	// needing update. This is done in a separate goroutine to avoid contention
	// between multiple controller workers.
	targetsNeedsUpdate chan struct{}
}

// Check that Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)
var _ brokerreconciler.Finalizer = (*Reconciler)(nil)

var brokerGVK = brokerv1beta1.SchemeGroupVersion.WithKind("Broker")

func newReconciledNormal(namespace, name string) pkgreconciler.Event {
	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerReconciled, "Broker reconciled: \"%s/%s\"", namespace, name)
}

func (r *Reconciler) ReconcileKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	if err := r.reconcileBroker(ctx, b); err != nil {
		logging.FromContext(ctx).Desugar().Error("Problem reconciling broker", zap.Error(err))
		return fmt.Errorf("failed to reconcile broker: %v", err)
		//TODO instead of returning on error, update the data plane configmap with
		// whatever info is available. or put this in a defer?
	}

	if err := r.reconcileTriggers(ctx, b); err != nil {
		logging.FromContext(ctx).Desugar().Error("Problem reconciling triggers", zap.Error(err))
		return fmt.Errorf("failed to reconcile triggers: %v", err)
	}

	logging.FromContext(ctx).Desugar().Info("targetsConfig", zap.Any("cfg", r.targetsConfig.String()))

	return newReconciledNormal(b.Namespace, b.Name)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Finalizing Broker")

	// Reconcile triggers so they update their status
	if err := r.reconcileTriggers(ctx, b); err != nil {
		logging.FromContext(ctx).Desugar().Error("Problem reconciling triggers", zap.Error(err))
		return fmt.Errorf("failed to reconcile triggers: %v", err)
	}

	if err := r.deleteDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("Failed to delete Pub/Sub topic: %v", err)
	}

	// TODO should we also delete the broker from the config? Maybe better
	// to keep the targets in place if there are still triggers. But we can
	// update the status to UNKNOWN and remove address etc. Maybe need a new
	// status DELETED or a deleted timestamp that can be used to clean up later.

	return newReconciledNormal(b.Namespace, b.Name)
}

func (r *Reconciler) reconcileBroker(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Reconciling Broker", zap.Any("broker", b))
	b.Status.InitializeConditions()
	b.Status.ObservedGeneration = b.Generation

	// Create decoupling topic and pullsub for this broker. Ingress will push
	// to this topic and fanout will pull from the pull sub.
	if err := r.reconcileDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("Decoupling topic reconcile failed: %w", err)
	}

	r.targetsConfig.MutateBroker(b.Namespace, b.Name, func(m config.BrokerMutation) {
		m.SetID(string(b.UID))
		m.SetAddress("") //TODO
		m.SetDecoupleQueue(&config.Queue{
			//TODO should this use the status fields or the name generator funcs?
			Topic:        b.Status.TopicID,
			Subscription: b.Status.SubscriptionID,
		})
		if b.Status.IsReady() {
			m.SetState(config.State_READY)
		} else {
			m.SetState(config.State_UNKNOWN)
		}
	})

	// Update config map
	r.flagTargetsForUpdate()

	//TODO configmap cleanup: if any brokers are in deleted state with no triggers
	// (or all triggers are in deleted state), remove that entry
	return nil
}

func (r *Reconciler) reconcileDecouplingTopicAndSubscription(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Reconciling decoupling topic")
	// get ProjectID from metadata
	projectID, err := utils.ProjectID("")
	if err != nil {
		logger.Error("Failed to find project id", zap.Error(err))
		return err
	}
	// Set the projectID in the status.
	b.Status.ProjectID = projectID

	client, err := r.CreateClientFn(ctx, projectID)
	if err != nil {
		logger.Error("Failed to create Pub/Sub client", zap.Error(err))
		return err
	}
	defer client.Close()

	// Check if topic exists, and if not, create it.
	topicID := resources.GenerateDecouplingTopicName(b)
	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub topic exists", zap.Error(err))
		return err
	}

	if !exists {
		topicConfig := &gpubsub.TopicConfig{
			Labels: map[string]string{
				"resource":     "brokers",
				"broker_class": brokerv1beta1.BrokerClass,
				"namespace":    b.Namespace,
				"name":         b.Name,
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
				b.Status.MarkTopicFailed("CreationFailed", "Topic creation failed: %w", err)
				return err
			}
			logger.Error("Failed to create Pub/Sub topic", zap.Error(err))
			b.Status.MarkTopicFailed("CreationFailed", "Topic creation failed: %w", err)
			return err
		}
		logger.Info("Created PubSub topic", zap.String("name", topic.ID()))
		r.Recorder.Eventf(b, corev1.EventTypeNormal, topicCreated, "Created PubSub topic %q", topic.ID())
	}

	b.Status.MarkTopicReady()
	// TODO(grantr): this isn't actually persisted due to webhook issues.
	b.Status.TopicID = topic.ID()

	// Check if PullSub exists, and if not, create it.
	subID := resources.GenerateDecouplingSubscriptionName(b)
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
				"resource":     "brokers",
				"broker_class": brokerv1beta1.BrokerClass,
				"namespace":    b.Namespace,
				"name":         b.Name,
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
			b.Status.MarkSubscriptionFailed("CreationFailed", "Subscription creation failed: %w", err)
			return err
		}
		logger.Info("Created PubSub subscription", zap.String("name", sub.ID()))
		r.Recorder.Eventf(b, corev1.EventTypeNormal, subCreated, "Created PubSub subscription %q", sub.ID())
	}
	//TODO update the subscription's config if needed.

	b.Status.MarkSubscriptionReady()
	// TODO(grantr): this isn't actually persisted due to webhook issues.
	b.Status.SubscriptionID = sub.ID()

	return nil
}

func (r *Reconciler) deleteDecouplingTopicAndSubscription(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx).Desugar()
	logger.Debug("Deleting decoupling topic")

	// get ProjectID from metadata
	projectID, err := utils.ProjectID("")
	if err != nil {
		logger.Error("Failed to find project id", zap.Error(err))
		return err
	}

	client, err := r.CreateClientFn(ctx, projectID)
	if err != nil {
		logger.Error("Failed to create Pub/Sub client", zap.Error(err))
		return err
	}
	defer client.Close()

	// Delete topic if it exists. Pull subscriptions continue pulling from the
	// topic until deleted themselves.
	topicID := resources.GenerateDecouplingTopicName(b)
	topic := client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		logger.Error("Failed to verify Pub/Sub topic exists", zap.Error(err))
		return err
	}
	if exists {
		if err := topic.Delete(ctx); err != nil {
			logger.Error("Failed to delete Pub/Sub topic", zap.Error(err))
			return err
		}
		logger.Info("Deleted PubSub topic", zap.String("name", topic.ID()))
		r.Recorder.Eventf(b, corev1.EventTypeNormal, topicDeleted, "Deleted PubSub topic %q", topic.ID())
	}

	// Delete pull subscription if it exists.
	// TODO could alternately set expiration policy to make pubsub delete it after some idle time.
	// https://cloud.google.com/pubsub/docs/admin#deleting_a_topic
	subID := resources.GenerateDecouplingSubscriptionName(b)
	sub := client.Subscription(subID)
	exists, err = sub.Exists(ctx)
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

	ctx = contextWithBroker(ctx, b)
	ctx = contextWithTargets(ctx, r.targetsConfig)
	for _, t := range triggers {
		if t.Spec.Broker == b.Name {
			logger := logging.FromContext(ctx).With(zap.String("trigger", t.Name), zap.String("broker", b.Name))
			ctx = logging.WithLogger(ctx, logger)

			if tKey, err := cache.MetaNamespaceKeyFunc(t); err == nil {
				err = r.triggerReconciler.Reconcile(ctx, tKey)
			}
		}
	}

	//TODO aggregate errors?
	return err
}

//TODO all this stuff should be in a configmap variant of the config object

// This function is not thread-safe and should only be executed by
// TargetsConfigUpdater
func (r *Reconciler) updateTargetsConfig(ctx context.Context) error {
	logger := r.Logger.Desugar()

	//Load the existing config first if it exists
	//TODO retry?
	r.loadConfigOnce.Do(func() { r.loadTargetsConfig(ctx) })
	//TODO resources package?
	data, err := r.targetsConfig.Bytes()
	if err != nil {
		return fmt.Errorf("error serializing targets config: %w", err)
	}
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetsCMName,
			Namespace: targetsCMNamespace,
		},
		BinaryData: map[string][]byte{targetsCMKey: data},
		// For debugging purposes
		Data: map[string]string{"targets.txt": r.targetsConfig.String()},
	}

	logger.Debug("Current targets config", zap.Any("targetsConfig", r.targetsConfig.String()))

	existing, err := r.configMapLister.ConfigMaps(desired.Namespace).Get(desired.Name)
	if errors.IsNotFound(err) {
		logger.Debug("Creating targets ConfigMap", zap.String("namespace", desired.Namespace), zap.String("name", desired.Name))
		_, err = r.KubeClientSet.CoreV1().ConfigMaps(desired.Namespace).Create(desired)
		if err != nil {
			return fmt.Errorf("error creating targets ConfigMap: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("error getting targets ConfigMap: %w", err)
	}

	logger.Debug("Compare targets ConfigMap", zap.Any("existing", base64.StdEncoding.EncodeToString(existing.BinaryData[targetsCMKey])), zap.String("desired", base64.StdEncoding.EncodeToString(desired.BinaryData[targetsCMKey])))
	if !equality.Semantic.DeepEqual(desired.BinaryData, existing.BinaryData) {
		logger.Debug("Updating targets ConfigMap")
		_, err = r.KubeClientSet.CoreV1().ConfigMaps(desired.Namespace).Update(desired)
		if err != nil {
			return fmt.Errorf("error updating targets ConfigMap: %w", err)
		}
	}
	return nil
}

func (r *Reconciler) loadTargetsConfig(ctx context.Context) error {
	r.Logger.Debug("Loading targets config from configmap")
	existing, err := r.configMapLister.ConfigMaps(targetsCMNamespace).Get(targetsCMName)
	if err != nil {
		return fmt.Errorf("error getting targets ConfigMap: %w", err)
	}

	targets, err := memory.NewTargetsFromBytes(existing.BinaryData[targetsCMKey])
	if err != nil {
		return fmt.Errorf("error loading targets from ConfigMap: %w", err)
	}
	r.targetsConfig = targets
	r.Logger.Debugw("Loaded targets config from ConfigMap", zap.String("resourceVersion", existing.ResourceVersion))
	return nil
}

func (r *Reconciler) TargetsConfigUpdater(ctx context.Context) {
	r.Logger.Debug("Starting TargetsConfigUpdater")
	// check every 10 seconds even if no reconciles have occurred
	ticker := time.NewTicker(targetsCMResyncPeriod)

	for {
		select {
		case <-ctx.Done():
			r.Logger.Debug("Stopping TargetsConfigUpdater")
			return
		case <-r.targetsNeedsUpdate:
			if err := r.updateTargetsConfig(ctx); err != nil {
				r.Logger.Error("Error in TargetsConfigUpdater: %w", err)
			}
		case <-ticker.C:
			if err := r.updateTargetsConfig(ctx); err != nil {
				r.Logger.Error("Error in TargetsConfigUpdater: %w", err)
			}
		}
	}
}

func (r *Reconciler) flagTargetsForUpdate() {
	select {
	case r.targetsNeedsUpdate <- struct{}{}:
		r.Logger.Debug("Flagged targets for update")
	default:
		r.Logger.Debug("Flagged targets for update but already flagged")
	}
}
