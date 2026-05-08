#!/bin/bash
# Run on worker-1, worker-2, worker-3
# Phase 1, Step 13: conntrack, kubeadm join, node labels
# Replace TOKEN and HASH with values printed by control-plane-init.sh
set -e

CONTROL_PLANE_IP="10.0.1.3"
TOKEN="<token-from-kubeadm-init>"
HASH="sha256:<hash-from-kubeadm-init>"

# conntrack — required by kubeadm preflight on workers too
sudo apt-get install -y -qq conntrack

# kubeadm join — contacts control plane, verifies CA cert via hash,
# gets permanent kubelet client cert, registers node
sudo kubeadm join ${CONTROL_PLANE_IP}:6443 \
  --token ${TOKEN} \
  --discovery-token-ca-cert-hash ${HASH}

echo "worker joined: $(hostname)"

# Apply node labels based on GCP instance metadata (set by Terraform)
# Labels are used by nodeSelector in workload manifests to pin pods to the right node:
#   worker-1 (node-role=kubeflow)    → Kubeflow Pipelines, Katib, Flower FL
#   worker-2 (node-role=kserve)      → KServe, Knative Eventing, MinIO
#   worker-3 (node-role=monitoring)  → Prometheus, Grafana, MLflow, PostgreSQL, agent
NODE_ROLE=$(curl -sf \
  "http://metadata.google.internal/computeMetadata/v1/instance/attributes/node-role" \
  -H "Metadata-Flavor: Google" || echo "")

if [ -n "$NODE_ROLE" ]; then
  NODE_NAME=$(hostname)
  # wait until node appears in API server
  until kubectl get node "$NODE_NAME" &>/dev/null; do sleep 3; done
  kubectl label node "$NODE_NAME" node-role="$NODE_ROLE" --overwrite
  echo "labelled $NODE_NAME node-role=$NODE_ROLE"
fi
