#!/bin/bash
# Run on LOCAL Mac
# Builds custom project images (fl, ml-trainer, ml-agent) for linux/amd64,
# transfers them to every cluster node via ctr images import.
#
# Also transfers third-party images that are unavailable from public registries
# (gcr.io/ml-pipeline images were removed; kserve/kube-rbac-proxy pulled locally).
#
# Usage:
#   bash scripts/build-and-load.sh [--third-party|--third-party-only]
#   --third-party       also pull and load third-party kubeflow/kserve images
#   --third-party-only  skip custom image build/load; only do third-party
set -e

ZONE="asia-south1-a"
PROJECT="gen-ai-pritha"
NODES=("control-plane" "worker-1" "worker-2")
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# helper: save image to tar, stream to node, import into containerd
load_image() {
  local image="$1"
  local node="$2"
  echo "  → loading ${image} on ${node}..."
  docker save "${image}" | gzip | \
    gcloud compute ssh "${node}" --zone="${ZONE}" --project="${PROJECT}" \
      --command="sudo ctr -n k8s.io images import -" 2>&1
}

# ──────────────────────────────────────────────────────────────────────────────
# 1. BUILD CUSTOM PROJECT IMAGES  (skipped with --third-party-only)
# ──────────────────────────────────────────────────────────────────────────────
if [[ "$1" == "--third-party-only" ]]; then
  echo "Skipping custom image build (--third-party-only)."
else

echo "=== Building custom images (linux/amd64) ==="

docker buildx build \
  --platform linux/amd64 \
  --load \
  -t fl:latest \
  -f "${PROJECT_DIR}/fl/Dockerfile" \
  "${PROJECT_DIR}"
echo "✓ fl:latest"

docker buildx build \
  --platform linux/amd64 \
  --load \
  -t ml-trainer:latest \
  -f "${PROJECT_DIR}/ml/Dockerfile" \
  "${PROJECT_DIR}"
echo "✓ ml-trainer:latest"

docker buildx build \
  --platform linux/amd64 \
  --load \
  -t ml-agent:latest \
  -f "${PROJECT_DIR}/agent/Dockerfile" \
  "${PROJECT_DIR}"
echo "✓ ml-agent:latest"

# ──────────────────────────────────────────────────────────────────────────────
# 2. LOAD CUSTOM IMAGES ON ALL NODES
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Loading custom images on cluster nodes ==="
for node in "${NODES[@]}"; do
  echo "Node: ${node}"
  load_image "fl:latest" "${node}"
  load_image "ml-trainer:latest" "${node}"
  load_image "ml-agent:latest" "${node}"
done

fi  # end --third-party-only skip block

# ──────────────────────────────────────────────────────────────────────────────
# 3. THIRD-PARTY IMAGES (opt-in with --third-party or --third-party-only)
#    These images are unavailable from their original registries:
#    - gcr.io/ml-pipeline/* — removed from Google Container Registry
#    - gcr.io/kubebuilder/kube-rbac-proxy — moved to registry.k8s.io
#    Solution: pull equivalent images locally, retag, and load on nodes.
# ──────────────────────────────────────────────────────────────────────────────
if [[ "$1" != "--third-party" && "$1" != "--third-party-only" ]]; then
  echo ""
  echo "Skipping third-party images. Run with --third-party to include them."
  echo "Done."
  exit 0
fi

echo ""
echo "=== Pulling third-party images (as single linux/amd64 images) ==="

# docker pull --platform linux/amd64 downloads only amd64 blobs but docker save
# still serializes the full multi-arch manifest index → ctr import fails with
# "unable to create manifests file: NotFound: content digest ... not found".
# Fix: use docker buildx build --load with a FROM Dockerfile to create a
# single-arch local image that docker save can serialize cleanly.
pull_amd64() {
  local src="$1"
  local dst="${2:-$1}"
  echo "  pulling ${src} as linux/amd64 → ${dst}"
  docker buildx build \
    --platform linux/amd64 \
    --load \
    -t "${dst}" \
    --build-arg SRC="${src}" \
    - <<'DOCKERFILE'
ARG SRC
FROM ${SRC}
DOCKERFILE
}

THIRD_PARTY_IMAGES=(
  "kserve/kserve-controller:v0.13.1"
  "kubeflownotebookswg/pvcviewer-controller:v1.9.2"
  "kubeflownotebookswg/tensorboard-controller:v1.9.2"
  "quay.io/minio/minio:RELEASE.2024-06-13T22-53-53Z"
  "istio/proxyv2:1.22.1"
)

# kube-rbac-proxy moved from gcr.io → registry.k8s.io
# Pull from registry.k8s.io then store under the gcr.io tag manifests expect
pull_amd64 registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.13.1 gcr.io/kubebuilder/kube-rbac-proxy:v0.13.1
pull_amd64 registry.k8s.io/kubebuilder/kube-rbac-proxy:v0.8.0  gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0
THIRD_PARTY_IMAGES+=(
  "gcr.io/kubebuilder/kube-rbac-proxy:v0.13.1"
  "gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0"
)

for img in "${THIRD_PARTY_IMAGES[@]}"; do
  pull_amd64 "${img}"
done

# gcr.io/ml-pipeline images no longer exist on gcr.io.
# Use quay.io/minio/minio as replacement for the kubeflow pipelines minio sidecar.
# The ml-pipeline/frontend image is patched in manifests to use nginx proxy.
docker tag quay.io/minio/minio:RELEASE.2024-06-13T22-53-53Z \
  gcr.io/ml-pipeline/minio:RELEASE.2019-08-14T20-37-41Z-license-compliance
THIRD_PARTY_IMAGES+=("gcr.io/ml-pipeline/minio:RELEASE.2019-08-14T20-37-41Z-license-compliance")

echo ""
echo "=== Loading third-party images on cluster nodes ==="
for node in "${NODES[@]}"; do
  echo "Node: ${node}"
  for img in "${THIRD_PARTY_IMAGES[@]}"; do
    load_image "${img}" "${node}"
  done
done

echo ""
echo "Done. All images loaded."
echo ""
echo "NOTE: gcr.io/ml-pipeline/frontend:2.3.0 has no public replacement."
echo "The ml-pipeline-ui deployment is patched to imagePullPolicy=Never,"
echo "using the nginx-based frontend image built by scripts/build-ml-pipeline-ui.sh"
