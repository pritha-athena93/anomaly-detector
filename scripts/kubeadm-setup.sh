#!/bin/bash
# Run on ALL nodes (control-plane, worker-1, worker-2)
# Phase 1, Step 10: install kubeadm, kubelet, kubectl
set -e

K8S_VERSION="1.31"

sudo apt-get install -y -qq apt-transport-https ca-certificates curl gpg
curl -fsSL https://pkgs.k8s.io/core:/stable:/v${K8S_VERSION}/deb/Release.key | \
  sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v${K8S_VERSION}/deb/ /" | \
  sudo tee /etc/apt/sources.list.d/kubernetes.list

sudo apt-get update -qq
sudo apt-get install -y -qq kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable --now kubelet

echo "kubeadm/kubelet/kubectl done: $(hostname) | $(kubeadm version -o short)"
