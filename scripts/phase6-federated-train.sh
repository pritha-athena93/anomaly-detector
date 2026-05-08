#!/bin/bash
# Run on control-plane
# Phase 6: Build FL image, deploy FL server + clients, run 10 federated rounds
# Prereq: Phase 5 data at ~/anomaly-detector/data/ on both workers
set -e

PROJECT_DIR=~/anomaly-detector

# Build FL Docker image on each node (imagePullPolicy: Never — no registry)
for node in control-plane worker-1 worker-2; do
  echo "Building fl image on ${node}..."
  gcloud compute ssh ${node} --zone=asia-south1-a --project=gen-ai-pritha \
    --command="cd ${PROJECT_DIR} && docker build -t fl:latest -f fl/Dockerfile ."
done

# Deploy FL server (waits for 2 clients before starting rounds)
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/federated-learning/fl-server.yaml
kubectl wait --for=condition=Ready pod -l app=fl-server -n federated-learning --timeout=120s

# Deploy both clients — they connect to fl-server automatically
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/federated-learning/fl-client.yaml
kubectl wait --for=condition=Ready pod -l app=fl-client-1 -n federated-learning --timeout=120s
kubectl wait --for=condition=Ready pod -l app=fl-client-2 -n federated-learning --timeout=120s

echo "FL training started. Follow progress:"
echo "  Server logs:   kubectl logs -f -l app=fl-server -n federated-learning"
echo "  Client 1 logs: kubectl logs -f -l app=fl-client-1 -n federated-learning"
echo "  Client 2 logs: kubectl logs -f -l app=fl-client-2 -n federated-learning"
echo "  MLflow:        http://<any-node-ip>:32500 → experiment: federated-anomaly-detection"
