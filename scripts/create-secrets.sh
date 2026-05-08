#!/bin/bash
# Run on control-plane BEFORE any phase install
# Reads credentials from env vars — never hardcode passwords here.
#
# Usage:
#   export POSTGRES_ROOT_PASSWORD=...
#   export MLFLOW_DB_USER=mlflow
#   export MLFLOW_DB_PASSWORD=...
#   export MINIO_ROOT_USER=minio
#   export MINIO_ROOT_PASSWORD=...
#   export GRAFANA_ADMIN_PASSWORD=...
#   export ANTHROPIC_API_KEY=...
#   bash scripts/create-secrets.sh
set -e

: "${POSTGRES_ROOT_PASSWORD:?POSTGRES_ROOT_PASSWORD not set}"
: "${MLFLOW_DB_USER:?MLFLOW_DB_USER not set}"
: "${MLFLOW_DB_PASSWORD:?MLFLOW_DB_PASSWORD not set}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER not set}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD not set}"
: "${GRAFANA_ADMIN_PASSWORD:?GRAFANA_ADMIN_PASSWORD not set}"
: "${ANTHROPIC_API_KEY:?ANTHROPIC_API_KEY not set}"

# PostgreSQL secret (bitnami chart reads from this)
kubectl create secret generic mlflow-postgres-secret \
  -n mlflow \
  --from-literal=postgres-password="${POSTGRES_ROOT_PASSWORD}" \
  --from-literal=password="${MLFLOW_DB_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# MinIO secret (bitnami chart reads from this)
kubectl create secret generic mlflow-minio-secret \
  -n mlflow \
  --from-literal=root-user="${MINIO_ROOT_USER}" \
  --from-literal=root-password="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# MLflow env secret (deployment.yaml envFrom references this)
kubectl create secret generic mlflow-env-secret \
  -n mlflow \
  --from-literal=BACKEND_STORE_URI="postgresql://${MLFLOW_DB_USER}:${MLFLOW_DB_PASSWORD}@mlflow-postgres-postgresql:5432/mlflow" \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Grafana admin secret (kube-prometheus-stack references this)
kubectl create secret generic grafana-admin-secret \
  -n monitoring \
  --from-literal=admin-user="admin" \
  --from-literal=admin-password="${GRAFANA_ADMIN_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

# Anthropic API key (ml-agent deployment references this)
kubectl create secret generic anthropic-secret \
  -n ml-agent \
  --from-literal=api-key="${ANTHROPIC_API_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

# KServe MinIO credentials (for model artifact access)
kubectl create secret generic kserve-minio-secret \
  -n kserve \
  --from-literal=AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl annotate secret kserve-minio-secret -n kserve \
  serving.kserve.io/s3-endpoint="http://mlflow-minio.mlflow.svc:9000" \
  serving.kserve.io/s3-usehttps="0" \
  serving.kserve.io/s3-verifyssl="0" \
  serving.kserve.io/s3-region="us-east-1" \
  --overwrite

# KServe ServiceAccount bound to minio credentials
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kserve-sa
  namespace: kserve
secrets:
  - name: kserve-minio-secret
EOF

echo "All secrets created"
