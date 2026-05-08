#!/bin/bash
# Run on LOCAL Mac
# Builds gcr.io/ml-pipeline/frontend:2.3.0 from source.
# Needed because Google removed all gcr.io/ml-pipeline images from public access.
#
# Prerequisites: Docker Desktop with BuildKit, ~4 GB free disk, ~10 min build time
set -e

ZONE="asia-south1-a"
PROJECT="gen-ai-pritha"
NODES=("control-plane" "worker-1" "worker-2")
IMAGE="gcr.io/ml-pipeline/frontend:2.3.0"
PIPELINES_TAG="2.3.0"
TMP_DIR="/tmp/kfp-frontend-build"

echo "=== Cloning kubeflow/pipelines at ${PIPELINES_TAG} (sparse checkout) ==="
rm -rf "${TMP_DIR}"
git clone \
  --depth=1 \
  --branch "${PIPELINES_TAG}" \
  --filter=blob:none \
  --sparse \
  https://github.com/kubeflow/pipelines.git \
  "${TMP_DIR}"

cd "${TMP_DIR}"
git sparse-checkout set frontend

# yarn-licenses.sh calls `yarn licenses generate-disclaimer` which was removed
# in yarn 2+. The step only generates attribution files, not needed for runtime.
# Patch it out before building.
sed -i 's|RUN ./scripts/yarn-licenses.sh|RUN echo "skipping yarn-licenses (yarn 2+ incompatible)"|' \
  "${TMP_DIR}/frontend/Dockerfile"

echo "=== Building ${IMAGE} for linux/amd64 ==="
docker buildx build \
  --platform linux/amd64 \
  --load \
  -t "${IMAGE}" \
  --build-arg COMMIT_HASH="${PIPELINES_TAG}" \
  --build-arg TAG_NAME="${PIPELINES_TAG}" \
  --build-arg DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  "${TMP_DIR}/frontend"

echo "✓ Built ${IMAGE}"

echo "=== Loading onto cluster nodes ==="
for node in "${NODES[@]}"; do
  echo "  → ${node}..."
  docker save "${IMAGE}" | gzip | \
    gcloud compute ssh "${node}" --zone="${ZONE}" --project="${PROJECT}" \
      --command="sudo ctr -n k8s.io images import -" 2>&1
done

echo ""
echo "Done. ${IMAGE} loaded on all nodes."
echo "Apply imagePullPolicy: Never patch:"
echo "  kubectl patch deployment ml-pipeline-ui -n kubeflow --type=json \\"
echo "    -p '[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"Never\"}]'"
