#!/bin/bash
# Run on control-plane
# Phase 8: Export best model from MLflow to MinIO, deploy KServe InferenceService
#
# Step 1: export model artifact from MLflow run to a known MinIO path
# Step 2: deploy InferenceService pointing to that path
#
# Prereq: Phase 7 complete, model registered as "anomaly-detector" in MLflow
set -e

PROJECT_DIR=~/anomaly-detector
MINIO_ENDPOINT=http://$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):30900

# Get best Production model version from MLflow
BEST_VERSION=$(python3 - <<'EOF'
import mlflow
import os
mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:32500"))
client = mlflow.MlflowClient()
versions = client.get_latest_versions("anomaly-detector", stages=["Production"])
if versions:
    print(versions[0].run_id)
else:
    # fallback: latest version regardless of stage
    all_v = client.search_model_versions("name='anomaly-detector'")
    print(sorted(all_v, key=lambda v: int(v.version))[-1].run_id)
EOF
)

echo "Best model run_id: ${BEST_VERSION}"

# Download model artifact from MLflow and re-upload to known MinIO path
# (KServe needs a predictable URI, not the MLflow experiment hash path)
pip install -q mlflow boto3

python3 - <<EOF
import mlflow, boto3, os, tempfile, shutil

run_id = "${BEST_VERSION}"
minio_endpoint = "${MINIO_ENDPOINT}"

mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:32500"))
with tempfile.TemporaryDirectory() as tmp:
    local_path = mlflow.artifacts.download_artifacts(
        run_id=run_id, artifact_path="model", dst_path=tmp
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
    # upload model.pkl (sklearn format) to a fixed path
    model_pkl = os.path.join(local_path, "model.pkl")
    s3.upload_file(model_pkl, "mlflow-artifacts", "anomaly-detector/model/model.pkl")
    print("Model uploaded to s3://mlflow-artifacts/anomaly-detector/model/model.pkl")
EOF

# Create KServe ServiceAccount + credentials
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/kserve/inference-service.yaml

echo "Waiting for InferenceService to become Ready..."
kubectl wait --for=condition=Ready inferenceservice anomaly-detector -n kserve --timeout=300s

echo "KServe endpoint ready."
echo ""
echo "Test inference:"
echo "  curl -X POST http://<any-node-ip>/v1/models/anomaly-detector:predict \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"instances\": [[0,0,14,2,1,0,50,100,200,3,42,1,2,0,0,16,0,1]]}'"
