#!/bin/bash
# Run on control-plane
# Phase 7: Build trainer image, submit Katib HPO experiment (10 trials, 2 parallel)
# Prereq: Phase 5 data at ~/anomaly-detector/data/processed/features.parquet
set -e

PROJECT_DIR=~/anomaly-detector

# Build trainer image on worker nodes (Katib schedules trial Jobs on workers)
for node in worker-1 worker-2; do
  echo "Building ml-trainer image on ${node}..."
  gcloud compute ssh ${node} --zone=asia-south1-a --project=gen-ai-pritha \
    --command="cd ${PROJECT_DIR} && docker build -t ml-trainer:latest -f ml/Dockerfile ."
done

# Submit Katib experiment
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/katib/experiment.yaml

echo "Katib experiment submitted."
echo ""
echo "Monitor:"
echo "  kubectl get experiment anomaly-hpo -n kubeflow"
echo "  kubectl get trials -n kubeflow"
echo "  Katib UI → Experiments tab (port-forward kubeflow dashboard)"
echo ""
echo "When complete, get best trial:"
echo "  kubectl get experiment anomaly-hpo -n kubeflow -o jsonpath='{.status.currentOptimalTrial}'"
echo ""
echo "Best model auto-logged to MLflow under 'anomaly-detector' registered model."
echo "Promote to Production:"
echo "  mlflow models transition-create --model-name anomaly-detector --version <v> --stage Production"
