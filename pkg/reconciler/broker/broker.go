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

// Package broker implements the Broker controller and reconciler reconciling
// Brokers and Triggers.
package broker

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"

	"knative.dev/eventing/pkg/apis/eventing"
	"knative.dev/eventing/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"

	brokerv1beta1 "github.com/google/knative-gcp/pkg/apis/broker/v1beta1"
	"github.com/google/knative-gcp/pkg/broker/config"
	"github.com/google/knative-gcp/pkg/broker/config/memory"
	brokerreconciler "github.com/google/knative-gcp/pkg/client/injection/reconciler/broker/v1beta1/broker"
	brokerlisters "github.com/google/knative-gcp/pkg/client/listers/broker/v1beta1"
	inteventslisters "github.com/google/knative-gcp/pkg/client/listers/intevents/v1alpha1"
	metadataClient "github.com/google/knative-gcp/pkg/gclient/metadata"
	"github.com/google/knative-gcp/pkg/reconciler"
	"github.com/google/knative-gcp/pkg/reconciler/broker/resources"
	brokercellresources "github.com/google/knative-gcp/pkg/reconciler/brokercell/resources"
	reconcilerutilspubsub "github.com/google/knative-gcp/pkg/reconciler/utils/pubsub"
	"github.com/google/knative-gcp/pkg/utils"
)

const (
	// Name of the corev1.Events emitted from the Broker reconciliation process.
	brokerReconciled  = "BrokerReconciled"
	brokerFinalized   = "BrokerFinalized"
	brokerCellCreated = "BrokerCellCreated"

	targetsCMName         = "broker-targets"
	targetsCMKey          = "targets"
	targetsCMResyncPeriod = 10 * time.Second
)

// Hard-coded for now. TODO(https://github.com/google/knative-gcp/issues/863)
// BrokerCell will handle this.
var dataPlaneDeployments = []string{
	brokercellresources.Name(resources.DefaultBroekrCellName, brokercellresources.IngressName),
	brokercellresources.Name(resources.DefaultBroekrCellName, brokercellresources.FanoutName),
	brokercellresources.Name(resources.DefaultBroekrCellName, brokercellresources.RetryName),
}

// TODO
// idea: assign broker resources to cell (configmap) in webhook based on a
// global configmap (in controller's namespace) of cell assignment rules, and
// label the broker with the assignment. Controller uses these labels to
// determine which configmap to create/update when a broker is reconciled, and
// to determine which brokers to reconcile when a configmap is updated.
// Initially, the assignment can be static.

type Reconciler struct {
	*reconciler.Base

	// listers index properties about resources
	triggerLister    brokerlisters.TriggerLister
	configMapLister  corev1listers.ConfigMapLister
	endpointsLister  corev1listers.EndpointsLister
	deploymentLister appsv1listers.DeploymentLister
	podLister        corev1listers.PodLister
	brokerCellLister inteventslisters.BrokerCellLister

	// TODO allow configuring multiples of these
	targetsConfig config.Targets

	// targetsNeedsUpdate is a channel that flags the targets ConfigMap as
	// needing update. This is done in a separate goroutine to avoid contention
	// between multiple controller workers.
	targetsNeedsUpdate chan struct{}

	projectID string

	// pubsubClient is used as the Pubsub client when present.
	pubsubClient *pubsub.Client
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

	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerReconciled, "Broker reconciled: \"%s/%s\"", b.Namespace, b.Name)
}

func (r *Reconciler) FinalizeKind(ctx context.Context, b *brokerv1beta1.Broker) pkgreconciler.Event {
	logger := logging.FromContext(ctx)
	logger.Debug("Finalizing Broker", zap.Any("broker", b))

	// Delete broker from targets-config, this will cause the data plane to stop working for this Broker and all
	// undelivered events will be lost.
	r.targetsConfig.MutateBroker(b.Namespace, b.Name, func(m config.BrokerMutation) {
		m.Delete()
	})

	if err := r.deleteDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("failed to delete Pub/Sub topic: %v", err)
	}

	return pkgreconciler.NewEvent(corev1.EventTypeNormal, brokerFinalized, "Broker finalized: \"%s/%s\"", b.Namespace, b.Name)
}

func (r *Reconciler) reconcileBroker(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx)
	logger.Debug("Reconciling Broker", zap.Any("broker", b))
	b.Status.InitializeConditions()
	b.Status.ObservedGeneration = b.Generation

	if err := r.ensureBrokerCellExists(ctx, b); err != nil {
		return fmt.Errorf("brokercell reconcile failed: %v", err)
	}

	// Create decoupling topic and pullsub for this broker. Ingress will push
	// to this topic and fanout will pull from the pull sub.
	if err := r.reconcileDecouplingTopicAndSubscription(ctx, b); err != nil {
		return fmt.Errorf("decoupling topic reconcile failed: %v", err)
	}

	// Filter by `eventing.knative.dev/broker: <name>` here
	// to get only the triggers for this broker. The trigger webhook will
	// ensure that triggers are always labeled with their broker name.
	triggers, err := r.triggerLister.Triggers(b.Namespace).List(labels.SelectorFromSet(map[string]string{eventing.BrokerLabelKey: b.Name}))
	if err != nil {
		logger.Error("Problem listing triggers", zap.Error(err))
		b.Status.MarkConfigUnknown("ListTriggerFailed", "Problem listing triggers: %w", err)
		return err
	}

	r.reconcileConfig(ctx, b, triggers)
	// Update config map
	r.flagTargetsForUpdate()
	b.Status.MarkConfigReady()
	return nil
}

// reconcileConfig reconstructs the data entry for the given broker in targets-config.
func (r *Reconciler) reconcileConfig(ctx context.Context, b *brokerv1beta1.Broker, triggers []*brokerv1beta1.Trigger) {
	// TODO Maybe get rid of BrokerMutation and add Delete() and Upsert(broker) methods to TargetsConfig. Now we always
	//  delete or update the entire broker entry and we don't need partial updates per trigger.
	// The code can be simplified to r.targetsConfig.Upsert(brokerConfigEntry)
	r.targetsConfig.MutateBroker(b.Namespace, b.Name, func(m config.BrokerMutation) {
		// First delete the broker entry.
		m.Delete()

		// Then reconstruct the broker entry and insert it
		m.SetID(string(b.UID))
		m.SetAddress(b.Status.Address.URL.String())
		m.SetDecoupleQueue(&config.Queue{
			Topic:        resources.GenerateDecouplingTopicName(b),
			Subscription: resources.GenerateDecouplingSubscriptionName(b),
		})
		if b.Status.IsReady() {
			m.SetState(config.State_READY)
		} else {
			m.SetState(config.State_UNKNOWN)
		}

		// Insert each Trigger to the config.
		for _, t := range triggers {
			if t.Spec.Broker == b.Name {
				target := &config.Target{
					Id:        string(t.UID),
					Name:      t.Name,
					Namespace: t.Namespace,
					Broker:    b.Name,
					Address:   t.Status.SubscriberURI.String(),
					RetryQueue: &config.Queue{
						Topic:        resources.GenerateRetryTopicName(t),
						Subscription: resources.GenerateRetrySubscriptionName(t),
					},
				}
				if t.Spec.Filter != nil && t.Spec.Filter.Attributes != nil {
					target.FilterAttributes = t.Spec.Filter.Attributes
				}
				if t.Status.IsReady() {
					target.State = config.State_READY
				} else {
					target.State = config.State_UNKNOWN
				}
				m.UpsertTargets(target)
			}
		}
	})
}

func (r *Reconciler) reconcileDecouplingTopicAndSubscription(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx)
	logger.Debug("Reconciling decoupling topic", zap.Any("broker", b))
	// get ProjectID from metadata if projectID isn't set
	projectID, err := utils.ProjectID(r.projectID, metadataClient.NewDefaultMetadataClient())
	if err != nil {
		logger.Error("Failed to find project id", zap.Error(err))
		b.Status.MarkTopicUnknown("ProjectIdNotFound", "Failed to find project id: %w", err)
		b.Status.MarkSubscriptionUnknown("ProjectIdNotFound", "Failed to find project id: %w", err)
		return err
	}
	// Set the projectID in the status.
	//TODO uncomment when eventing webhook allows this
	//b.Status.ProjectID = projectID

	client := r.pubsubClient
	if client == nil {
		var err error
		client, err = pubsub.NewClient(ctx, projectID)
		if err != nil {
			logger.Error("Failed to create Pub/Sub client", zap.Error(err))
			b.Status.MarkTopicUnknown("PubSubClientCreationFailed", "Failed to create Pub/Sub client: %w", err)
			return err
		}
		defer client.Close()
	}
	pubsubReconciler := reconcilerutilspubsub.NewReconciler(client, r.Recorder)

	labels := map[string]string{
		"resource":     "brokers",
		"broker_class": brokerv1beta1.BrokerClass,
		"namespace":    b.Namespace,
		"name":         b.Name,
		//TODO add resource labels, but need to be sanitized: https://cloud.google.com/pubsub/docs/labels#requirements
	}

	// Check if topic exists, and if not, create it.
	topicID := resources.GenerateDecouplingTopicName(b)
	topicConfig := &pubsub.TopicConfig{Labels: labels}
	topic, err := pubsubReconciler.ReconcileTopic(ctx, topicID, topicConfig, b, &b.Status)
	if err != nil {
		return err
	}
	logger.Info("===================Status after TOPIC reconciliation:", zap.Any("string", b.Status.GetCondition(brokerv1beta1.BrokerConditionTopic)))
	// TODO(grantr): this isn't actually persisted due to webhook issues.
	//TODO uncomment when eventing webhook allows this
	//TODO uncomment when eventing webhook allows this
	//b.Status.TopicID = topic.ID()

	// Check if PullSub exists, and if not, create it.
	subID := resources.GenerateDecouplingSubscriptionName(b)
	subConfig := pubsub.SubscriptionConfig{
		Topic:  topic,
		Labels: labels,
		//TODO(grantr): configure these settings?
		// AckDeadline
		// RetentionDuration
	}
	if _, err := pubsubReconciler.ReconcileSubscription(ctx, subID, subConfig, b, &b.Status); err != nil {
		return err
	}

	// TODO(grantr): this isn't actually persisted due to webhook issues.
	//TODO uncomment when eventing webhook allows this
	//b.Status.SubscriptionID = sub.ID()

	return nil
}

func (r *Reconciler) deleteDecouplingTopicAndSubscription(ctx context.Context, b *brokerv1beta1.Broker) error {
	logger := logging.FromContext(ctx)
	logger.Debug("Deleting decoupling topic")

	// get ProjectID from metadata if projectID isn't set
	projectID, err := utils.ProjectID(r.projectID, metadataClient.NewDefaultMetadataClient())
	if err != nil {
		logger.Error("Failed to find project id", zap.Error(err))
		return err
	}

	client := r.pubsubClient
	if client == nil {
		client, err := pubsub.NewClient(ctx, projectID)
		if err != nil {
			logger.Error("Failed to create Pub/Sub client", zap.Error(err))
			return err
		}
		defer client.Close()
	}
	pubsubReconciler := reconcilerutilspubsub.NewReconciler(client, r.Recorder)

	// Delete topic if it exists. Pull subscriptions continue pulling from the
	// topic until deleted themselves.
	topicID := resources.GenerateDecouplingTopicName(b)
	err = multierr.Append(nil, pubsubReconciler.DeleteTopic(ctx, topicID, b))
	subID := resources.GenerateDecouplingSubscriptionName(b)
	err = multierr.Append(err, pubsubReconciler.DeleteSubscription(ctx, subID, b))

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
	if apierrs.IsNotFound(err) {
		r.Logger.Debug("Creating targets ConfigMap", zap.String("namespace", desired.Namespace), zap.String("name", desired.Name))
		existing, err = r.KubeClientSet.CoreV1().ConfigMaps(desired.Namespace).Create(desired)
		if err != nil {
			return fmt.Errorf("error creating targets ConfigMap: %w", err)
		}
		if err := r.updateConfigmapVolumeAnnotation(); err != nil {
			// Failing to update the annotation on the data plane pods means there
			// may be a longer propagation delay for the configmap volume to be
			// refreshed. But this is not treated as an error.
			r.Logger.Warnf("Error reconciling data plane deployments: %v", err)
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
		if err := r.updateConfigmapVolumeAnnotation(); err != nil {
			// Failing to update the annotation on the data plane pods means there
			// may be a longer propagation delay for the configmap volume to be
			// refreshed. But this is not treated as an error.
			r.Logger.Warnf("Error reconciling data plane deployments: %v", err)
		}
	}
	return nil
}

// TODO(https://github.com/google/knative-gcp/issues/863) With BrokerCell, we
// will reconcile data plane deployments dynamically.
func (r *Reconciler) updateConfigmapVolumeAnnotation() error {
	var err error
	for _, name := range dataPlaneDeployments {
		err = multierr.Append(err, resources.UpdateVolumeGenerationForDeployment(r.KubeClientSet, r.deploymentLister, r.podLister, system.Namespace(), name))
	}
	return err
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
		if apierrs.IsNotFound(err) {
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
	// check every 10 seconds even if no reconciles have occurred
	ticker := time.NewTicker(targetsCMResyncPeriod)

	//TODO configmap cleanup: if any brokers are in deleted state with no triggers
	// (or all triggers are in deleted state), remove that entry

	for {
		select {
		case <-ctx.Done():
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
