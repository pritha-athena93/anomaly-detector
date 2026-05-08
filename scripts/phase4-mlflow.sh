#!/bin/bash
# Run on control-plane
# Phase 4: PostgreSQL + MinIO (standalone) + MLflow tracking server
# Prereq: scripts/create-secrets.sh already run
# PostgreSQL: mlflow-postgres-postgresql svc, port 5432
# MinIO: NodePort API=30900, Console=30901
# MLflow: NodePort 32500
set -e

PROJECT_DIR=~/anomaly-detector

helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update

# StorageClass — local-path-provisioner for dynamic PVC provisioning
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.28/deploy/local-path-storage.yaml
kubectl wait --for=condition=Available deployment/local-path-provisioner \
  -n local-path-storage --timeout=120s
kubectl patch storageclass local-path \
  -p '{"metadata": {"annotations":{"storageclass.kubernetes.io/is-default-class":"true"}}}'

# PostgreSQL backend for MLflow
helm install mlflow-postgres bitnami/postgresql \
  -n mlflow \
  -f ${PROJECT_DIR}/k8s/helm/postgresql/values.yaml

kubectl wait --for=condition=Ready pod \
  -l app.kubernetes.io/name=postgresql \
  -n mlflow --timeout=300s

# MinIO — standalone deployment using official quay.io/minio/minio image
# (bitnami/minio images have availability issues on Docker Hub)
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/minio/deployment.yaml

kubectl wait --for=condition=Ready pod \
  -l app=mlflow-minio \
  -n mlflow --timeout=120s

# Create mlflow-artifacts bucket
MINIO_CLUSTERIP=$(kubectl get svc mlflow-minio -n mlflow \
  -o jsonpath='{.spec.clusterIP}')
kubectl run minio-init \
  --image=quay.io/minio/mc:RELEASE.2024-05-24T09-08-49Z \
  --restart=Never --rm -i -n mlflow \
  --command -- /bin/sh -c \
    "mc alias set myminio http://${MINIO_CLUSTERIP}:9000 \$(MINIO_ROOT_USER) \$(MINIO_ROOT_PASSWORD) && \
     mc mb --ignore-existing myminio/mlflow-artifacts && \
     echo BUCKET_OK" \
  --env="MINIO_ROOT_USER=$(kubectl get secret mlflow-minio-secret -n mlflow -o jsonpath='{.data.root-user}' | base64 -d)" \
  --env="MINIO_ROOT_PASSWORD=$(kubectl get secret mlflow-minio-secret -n mlflow -o jsonpath='{.data.root-password}' | base64 -d)"

# MLflow tracking server
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/mlflow/deployment.yaml

kubectl wait --for=condition=Ready pod \
  -l app=mlflow \
  -n mlflow --timeout=300s

echo "Phase 4 done"
echo "MLflow UI:     http://<any-node-ip>:32500"
echo "MinIO Console: http://<any-node-ip>:30901"
echo "MinIO API:     http://<any-node-ip>:30900"
