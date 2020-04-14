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
	"context"

	"knative.dev/pkg/apis"
)

const (
	brokerLabel = "eventing.knative.dev/broker"
)

// SetDefaults sets the default field values for a Trigger.
func (t *Trigger) SetDefaults(ctx context.Context) {
	withNS := apis.WithinParent(ctx, t.ObjectMeta)
	t.Spec.SetDefaults(withNS)
	setLabels(t)
}

func setLabels(t *Trigger) {
	if len(t.Labels) == 0 {
		t.Labels = map[string]string{}
	}
	t.Labels[brokerLabel] = t.Spec.Broker
}
