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

package v1beta1

import (
	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"
)

var triggerCondSet = apis.NewLivingConditionSet(
	TriggerConditionBroker,
	TriggerConditionTopic,
	TriggerConditionSubscription,
	TriggerConditionDependency,
	TriggerConditionSubscriberResolved,
)

const (
	// TriggerConditionReady has status True when all subconditions below have been set to True.
	TriggerConditionReady = apis.ConditionReady

	TriggerConditionBroker apis.ConditionType = "BrokerReady"

	TriggerConditionTopic apis.ConditionType = "TopicReady"

	TriggerConditionSubscription apis.ConditionType = "SubscriptionReady"

	TriggerConditionDependency apis.ConditionType = "DependencyReady"

	TriggerConditionSubscriberResolved apis.ConditionType = "SubscriberResolved"

	// TriggerAnyFilter Constant to represent that we should allow anything.
	TriggerAnyFilter = ""
)

// GetCondition returns the condition currently associated with the given type, or nil.
func (ts *TriggerStatus) GetCondition(t apis.ConditionType) *apis.Condition {
	return triggerCondSet.Manage(ts).GetCondition(t)
}

// GetTopLevelCondition returns the top level Condition.
func (ts *TriggerStatus) GetTopLevelCondition() *apis.Condition {
	return triggerCondSet.Manage(ts).GetTopLevelCondition()
}

// IsReady returns true if the resource is ready overall.
func (ts *TriggerStatus) IsReady() bool {
	return triggerCondSet.Manage(ts).IsHappy()
}

// InitializeConditions sets relevant unset conditions to Unknown state.
func (ts *TriggerStatus) InitializeConditions() {
	triggerCondSet.Manage(ts).InitializeConditions()
}

func (ts *TriggerStatus) PropagateBrokerStatus(bs *BrokerStatus) {
	bc := bs.GetTopLevelCondition()
	if bc == nil {
		ts.MarkBrokerNotConfigured()
		return
	}

	switch {
	case bc.Status == corev1.ConditionUnknown:
		ts.MarkBrokerUnknown(bc.Reason, bc.Message)
	case bc.Status == corev1.ConditionTrue:
		triggerCondSet.Manage(ts).MarkTrue(TriggerConditionBroker)
	case bc.Status == corev1.ConditionFalse:
		ts.MarkBrokerFailed(bc.Reason, bc.Message)
	default:
		ts.MarkBrokerUnknown("BrokerUnknown", "The status of Broker is invalid: %v", bc.Status)
	}
}

func (ts *TriggerStatus) MarkBrokerFailed(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkFalse(TriggerConditionBroker, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkBrokerUnknown(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkUnknown(TriggerConditionBroker, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkBrokerNotConfigured() {
	triggerCondSet.Manage(ts).MarkUnknown(TriggerConditionBroker,
		"BrokerNotConfigured", "Broker has not yet been reconciled.")
}

func (bs *TriggerStatus) MarkTopicFailed(reason, format string, args ...interface{}) {
	triggerCondSet.Manage(bs).MarkFalse(TriggerConditionTopic, reason, format, args...)
}

func (bs *TriggerStatus) MarkTopicReady() {
	triggerCondSet.Manage(bs).MarkTrue(TriggerConditionTopic)
}

func (bs *TriggerStatus) MarkSubscriptionFailed(reason, format string, args ...interface{}) {
	triggerCondSet.Manage(bs).MarkFalse(TriggerConditionSubscription, reason, format, args...)
}

func (bs *TriggerStatus) MarkSubscriptionReady() {
	triggerCondSet.Manage(bs).MarkTrue(TriggerConditionSubscription)
}

func (ts *TriggerStatus) MarkSubscriberResolvedSucceeded() {
	triggerCondSet.Manage(ts).MarkTrue(TriggerConditionSubscriberResolved)
}

func (ts *TriggerStatus) MarkSubscriberResolvedFailed(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkFalse(TriggerConditionSubscriberResolved, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkSubscriberResolvedUnknown(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkUnknown(TriggerConditionSubscriberResolved, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkDependencySucceeded() {
	triggerCondSet.Manage(ts).MarkTrue(TriggerConditionDependency)
}

func (ts *TriggerStatus) MarkDependencyFailed(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkFalse(TriggerConditionDependency, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkDependencyUnknown(reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkUnknown(TriggerConditionDependency, reason, messageFormat, messageA...)
}

func (ts *TriggerStatus) MarkDependencyNotConfigured() {
	triggerCondSet.Manage(ts).MarkUnknown(TriggerConditionDependency,
		"DependencyNotConfigured", "Dependency has not yet been reconciled.")
}

func (ts *TriggerStatus) PropagateDependencyStatus(ks *duckv1.KResource) {
	kc := ks.Status.GetCondition(apis.ConditionReady)
	if kc == nil {
		ts.MarkDependencyNotConfigured()
		return
	}

	switch {
	case kc.Status == corev1.ConditionUnknown:
		ts.MarkDependencyUnknown(kc.Reason, kc.Message)
	case kc.Status == corev1.ConditionTrue:
		ts.MarkDependencySucceeded()
	case kc.Status == corev1.ConditionFalse:
		ts.MarkDependencyFailed(kc.Reason, kc.Message)
	default:
		ts.MarkDependencyUnknown("DependencyUnknown", "The status of Dependency is invalid: %v", kc.Status)
	}
}
