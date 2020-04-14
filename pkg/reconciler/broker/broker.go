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
	"time"

	"github.com/gogo/protobuf/proto"
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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"knative.dev/eventing/pkg/logging"
	"knative.dev/eventing/pkg/reconciler/names"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/controller"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"
)

const (
	// Name of the corev1.Events emitted from the Broker reconciliation process.
	brokerReconcileError = "BrokerReconcileError"
	brokerReconciled     = "BrokerReconciled"
	brokerFinalized      = "BrokerFinalized"
	topicCreated         = "TopicCreated"
	subCreated           = "SubscriptionCreated"
	topicDeleted         = "TopicDeleted"
	subDeleted           = "SubscriptionDeleted"

	targetsCMName         = "broker-targets"
	targetsCMKey          = "targets"
	targetsCMResyncPeriod = 10 * time.Second

	ingressServiceName = "broker-ingress"
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
	targetsConfig config.Targets

	// targetsNeedsUpdate is a channel that flags the targets ConfigMap as
	// needing update. This is done in a separate goroutine to avoid contention
	// between multiple controller workers.
	targetsNeedsUpdate chan struct{}

	// projectID is used as the GCP project ID when present, skipping the
	// metadata server check. Used by tests.
	projectID string
}

// Check that Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)
var _ brokerreconciler.Finalizer = (*Reconciler)(nil)

var brokerGVK = brokerv1beta1.SchemeGroupVersion.WithKind("Broker")

func (r *Reconciler) ReconcileKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	if err := r.reconcileBroker(ctx, b); err != nil {
		logging.FromContext(ctx).Error("Problem reconciling broker", zap.Error(err))
		return fmt.Errorf("failed to reconcile broker: %w", err)
		//TODO instead of returning on error, update the data plane configmap with
		// whatever info is available. or put this in a defer?
	}

	if err := r.reconcileTriggers(ctx, b); err != nil {
		logging.FromContext(ctx).Error("Problem reconciling triggers", zap.Error(err))
		return fmt.Errorf("failed to reconcile triggers: %w", err)
	}

	logging.FromContext(ctx).Info("targetsConfig", zap.Any("cfg", r.targetsConfig.String()))

	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerReconciled, "Broker reconciled: \"%s/%s\"", b.Namespace, b.Name)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debug("Finalizing Broker", zap.Any("broker", b))

	// Reconcile triggers so they update their status
	if err := r.reconcileTriggers(ctx, b); err != nil {
		logger.Error("Problem reconciling triggers", zap.Error(err))
		return fmt.Errorf("failed to reconcile triggers: %w", err)
	}

	if err := r.deleteDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("Failed to delete Pub/Sub topic: %w", err)
	}

	// TODO should we also delete the broker from the config? Maybe better
	// to keep the targets in place if there are still triggers. But we can
	// update the status to UNKNOWN and remove address etc. Maybe need a new
	// status DELETED or a deleted timestamp that can be used to clean up later.

	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerFinalized, "Broker finalized: \"%s/%s\"", b.Namespace, b.Name)

}

func (r *Reconciler) reconcileBroker(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx)
	logger.Debug("Reconciling Broker", zap.Any("broker", b))
	b.Status.InitializeConditions()
	b.Status.ObservedGeneration = b.Generation

	// Create decoupling topic and pullsub for this broker. Ingress will push
	// to this topic and fanout will pull from the pull sub.
	if err := r.reconcileDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("Decoupling topic reconcile failed: %w", err)
	}

	//TODO in a webhook annotate the broker object with its ingress details
	// Route to shared ingress with namespace/name in the path as the broker
	// identifier.
	b.Status.SetAddress(&apis.URL{
		Scheme: "http",
		Host:   names.ServiceHostName("broker-ingress", system.Namespace()),
		Path:   fmt.Sprintf("/%s/%s", b.Namespace, b.Name),
	})

	// Verify the ingress service is healthy via endpoints.
	ingressEndpoints, err := r.endpointsLister.Endpoints(system.Namespace()).Get(ingressServiceName)
	if err != nil {
		logger.Error("Problem getting endpoints for ingress", zap.String("namespace", system.Namespace()), zap.Error(err))
		b.Status.MarkIngressFailed("ServiceFailure", "%v", err)
		return err
	}
	b.Status.PropagateIngressAvailability(ingressEndpoints)

	r.targetsConfig.MutateBroker(b.Namespace, b.Name, func(m config.BrokerMutation) {
		m.SetID(string(b.UID))
		m.SetAddress(b.Status.Address.URL.String())
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
	// TODO should this happen in a defer so there's an update regardless of
	// error status, or only when reconcile is successful?
	r.flagTargetsForUpdate()
	return nil
}

func (r *Reconciler) reconcileDecouplingTopicAndSubscription(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx)
	logger.Debug("Reconciling decoupling topic", zap.Any("broker", b))
	// get ProjectID from metadata if projectID isn't set
	projectID, err := utils.ProjectID(r.projectID)
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
	logger := logging.FromContext(ctx)
	logger.Debug("Deleting decoupling topic")

	// get ProjectID from metadata if projectID isn't set
	projectID, err := utils.ProjectID(r.projectID)
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

	// TODO(grantr): We could filter by `eventing.knative.dev/broker: name` here
	// to get only the triggers for this broker. The problem is that if users
	// change the label themselves, that trigger won't get reconciled even
	// though the broker field value is correct.
	//
	// Since this query is going to the lister instead of the apiserver, adding
	// a label selector here is just moving the client-side filter to a
	// different place with equivalent efficiency. Instead, list all triggers
	// in the namespace and
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
	//TODO resources package?
	data, err := r.targetsConfig.Bytes()
	if err != nil {
		return fmt.Errorf("error serializing targets config: %w", err)
	}
	desired := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      targetsCMName,
			Namespace: system.Namespace(),
		},
		BinaryData: map[string][]byte{targetsCMKey: data},
		// Write out the text version for debugging purposes only
		Data: map[string]string{"targets.txt": r.targetsConfig.String()},
	}

	r.Logger.Debug("Current targets config", zap.Any("targetsConfig", r.targetsConfig.String()))

	existing, err := r.configMapLister.ConfigMaps(desired.Namespace).Get(desired.Name)
	if errors.IsNotFound(err) {
		r.Logger.Debug("Creating targets ConfigMap", zap.String("namespace", desired.Namespace), zap.String("name", desired.Name))
		_, err = r.KubeClientSet.CoreV1().ConfigMaps(desired.Namespace).Create(desired)
		if err != nil {
			return fmt.Errorf("error creating targets ConfigMap: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("error getting targets ConfigMap: %w", err)
	}

	r.Logger.Debug("Compare targets ConfigMap", zap.Any("existing", base64.StdEncoding.EncodeToString(existing.BinaryData[targetsCMKey])), zap.String("desired", base64.StdEncoding.EncodeToString(desired.BinaryData[targetsCMKey])))
	if !equality.Semantic.DeepEqual(desired.BinaryData, existing.BinaryData) {
		r.Logger.Debug("Updating targets ConfigMap")
		_, err = r.KubeClientSet.CoreV1().ConfigMaps(desired.Namespace).Update(desired)
		if err != nil {
			return fmt.Errorf("error updating targets ConfigMap: %w", err)
		}
	}
	return nil
}

// LoadTargetsConfig retrieves the targets ConfigMap and
// populates the targets config struct.
func (r *Reconciler) LoadTargetsConfig(ctx context.Context) error {
	r.Logger.Debug("Loading targets config from configmap")
	//TODO should we use the apiserver here?
	// kubeclient.Get(ctx).CoreV1().ConfigMaps(system.Namespace()).Get(targetsCMName. metav1.GetOptions{})
	//TODO retry with wait.ExponentialBackoff
	existing, err := r.configMapLister.ConfigMaps(system.Namespace()).Get(targetsCMName)
	if err != nil {
		if errors.IsNotFound(err) {
			r.targetsConfig = memory.NewEmptyTargets()
			return nil
		}
		return fmt.Errorf("error getting targets ConfigMap: %w", err)
	}

	targets := &config.TargetsConfig{}
	data := existing.BinaryData[targetsCMKey]
	if err := proto.Unmarshal(data, targets); err != nil {
		return err
	}

	r.targetsConfig = memory.NewTargets(targets)
	r.Logger.Debug("Loaded targets config from ConfigMap", zap.String("resourceVersion", existing.ResourceVersion))
	return nil
}

func (r *Reconciler) TargetsConfigUpdater(ctx context.Context) {
	r.Logger.Debug("Starting TargetsConfigUpdater")
	// check every 10 seconds even if no reconciles have occurred
	ticker := time.NewTicker(targetsCMResyncPeriod)

	//TODO configmap cleanup: if any brokers are in deleted state with no triggers
	// (or all triggers are in deleted state), remove that entry

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
