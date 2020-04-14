#!/usr/bin/env bash
# Creates a placeholder configmap so that ingress and fanout can mount.
# We should probably let the controller to create the configmap as well as the data plane deployments.
kubectl create configmap broker-targets --from-file $(dirname $0)/targets
