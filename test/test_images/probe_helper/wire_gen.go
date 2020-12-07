// Code generated by Wire. DO NOT EDIT.

//go:generate wire
//+build !wireinject

package main

import (
	"context"
	"github.com/google/knative-gcp/pkg/utils/clients"
	"github.com/google/knative-gcp/test/test_images/probe_helper/probe"
	"github.com/google/knative-gcp/test/test_images/probe_helper/probe/handlers"
	"time"
)

// Injectors from wire.go:

func InitializeProbeHelper(ctx context.Context, brokerCellBaseUrl string, projectID clients.ProjectID, schedulerStaleDuration time.Duration, helperEnv probe.EnvConfig, forwardPort probe.ForwardPort, receivePort probe.ReceivePort) (*probe.Helper, error) {
	forwardClientOptions := probe.NewCeReceiverClientOptions(receivePort)
	ceForwardClient, err := probe.NewCeForwardClient(forwardClientOptions)
	if err != nil {
		return nil, err
	}
	brokerE2EDeliveryProbe := handlers.NewBrokerE2EDeliveryProbe(brokerCellBaseUrl, ceForwardClient)
	client, err := probe.NewPubSubClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	cePubSubClient, err := probe.NewCePubSubClient(ctx, client)
	if err != nil {
		return nil, err
	}
	cloudPubSubSourceProbe := handlers.NewCloudPubSubSourceProbe(cePubSubClient)
	storageClient, err := probe.NewStorageClient(ctx)
	if err != nil {
		return nil, err
	}
	cloudStorageSourceProbe := handlers.NewCloudStorageSourceProbe(storageClient)
	cloudStorageSourceCreateProbe := &handlers.CloudStorageSourceCreateProbe{
		CloudStorageSourceProbe: cloudStorageSourceProbe,
	}
	cloudStorageSourceUpdateMetadataProbe := &handlers.CloudStorageSourceUpdateMetadataProbe{
		CloudStorageSourceProbe: cloudStorageSourceProbe,
	}
	cloudStorageSourceArchiveProbe := &handlers.CloudStorageSourceArchiveProbe{
		CloudStorageSourceProbe: cloudStorageSourceProbe,
	}
	cloudStorageSourceDeleteProbe := &handlers.CloudStorageSourceDeleteProbe{
		CloudStorageSourceProbe: cloudStorageSourceProbe,
	}
	cloudAuditLogsSourceProbe := handlers.NewCloudAuditLogsSourceProbe(projectID, client)
	cloudSchedulerSourceProbe := handlers.NewCloudSchedulerSourceProbe(schedulerStaleDuration)
	eventTypeProbe := handlers.NewEventTypeHandler(brokerE2EDeliveryProbe, cloudPubSubSourceProbe, cloudStorageSourceCreateProbe, cloudStorageSourceUpdateMetadataProbe, cloudStorageSourceArchiveProbe, cloudStorageSourceDeleteProbe, cloudAuditLogsSourceProbe, cloudSchedulerSourceProbe)
	livenessChecker := handlers.NewLivenessChecker(cloudSchedulerSourceProbe)
	receiveClientOptions := probe.NewCeForwardClientOptions(forwardPort)
	ceReceiveClient, err := probe.NewCeReceiverClient(ctx, livenessChecker, receiveClientOptions)
	if err != nil {
		return nil, err
	}
	helper := probe.NewHelper(helperEnv, eventTypeProbe, ceForwardClient, ceReceiveClient, livenessChecker)
	return helper, nil
}