#!/bin/bash
# Run on control-plane
# Phase 9: Build agent image, apply RBAC, deploy LangGraph agent
# Prereq: anthropic-secret created by scripts/create-secrets.sh
set -e

PROJECT_DIR=~/anomaly-detector

# Build agent image on control-plane (agent runs on any node)
echo "Building ml-agent image..."
docker build -t ml-agent:latest -f ${PROJECT_DIR}/agent/Dockerfile ${PROJECT_DIR}

# Apply RBAC (ServiceAccount + ClusterRole + ClusterRoleBinding)
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/ml-agent/rbac.yaml

# Deploy agent
kubectl apply -f ${PROJECT_DIR}/k8s/manifests/ml-agent/deployment.yaml
kubectl wait --for=condition=Ready pod -l app=ml-agent -n ml-agent --timeout=120s

echo "Agent deployed."
echo ""
echo "Agent API: http://<any-node-ip>:32600"
echo ""
echo "Test queries:"
echo '  curl -X POST http://<any-node-ip>:32600/query \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '"'"'{"question": "What is the current cluster CPU utilisation?"}'"'"
echo ""
echo '  curl -X POST http://<any-node-ip>:32600/query \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '"'"'{"question": "Show me the last 5 training runs and their F1 scores"}'"'"
echo ""
echo '  curl -X POST http://<any-node-ip>:32600/query \'
echo '    -H "Content-Type: application/json" \'
echo '    -d '"'"'{"question": "Are there any unhealthy pods in the kubeflow namespace?"}'"'"
