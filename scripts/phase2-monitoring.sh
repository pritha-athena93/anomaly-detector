#!/bin/bash
# Run on control-plane
# Phase 2: install kube-prometheus-stack (Prometheus + Grafana)
# Grafana NodePort: 32300, password: admin
set -e

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  -n monitoring \
  -f ~/anomaly-detector/k8s/helm/kube-prometheus-stack/values.yaml

# wait for pods
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=grafana -n monitoring --timeout=300s

echo "Phase 2 done"
echo "Grafana: http://<any-node-ip>:32300  user=admin  pass=admin"
echo "Prometheus: http://<any-node-ip>:30090 (default ClusterIP — port-forward to access)"
