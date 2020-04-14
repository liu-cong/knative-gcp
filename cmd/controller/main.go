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

package main

import (
	"github.com/google/knative-gcp/pkg/reconciler/events/build"
	// The following line to load the gcp plugin (only required to authenticate against GKE clusters).
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"github.com/google/knative-gcp/pkg/reconciler/broker"
	"github.com/google/knative-gcp/pkg/reconciler/deployment"
	"github.com/google/knative-gcp/pkg/reconciler/events/auditlogs"
	"github.com/google/knative-gcp/pkg/reconciler/events/pubsub"
	"github.com/google/knative-gcp/pkg/reconciler/events/scheduler"
	"github.com/google/knative-gcp/pkg/reconciler/events/storage"
	"github.com/google/knative-gcp/pkg/reconciler/messaging/channel"
	kedapullsubscription "github.com/google/knative-gcp/pkg/reconciler/pubsub/pullsubscription/keda"
	staticpullsubscription "github.com/google/knative-gcp/pkg/reconciler/pubsub/pullsubscription/static"
	"github.com/google/knative-gcp/pkg/reconciler/pubsub/topic"

	"knative.dev/pkg/injection/sharedmain"
)

func main() {
	sharedmain.Main("controller",
		auditlogs.NewController,
		storage.NewController,
		scheduler.NewController,
		pubsub.NewController,
		build.NewController,
		staticpullsubscription.NewController,
		kedapullsubscription.NewController,
		topic.NewController,
		channel.NewController,
		deployment.NewController,
		broker.NewController,
	)
}
