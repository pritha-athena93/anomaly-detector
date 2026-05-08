#!/bin/bash
# Run on control-plane ONLY
# Phase 1, Steps 11-12: conntrack, kubeadm init, kubeconfig, helm, Calico CNI
# Phase 2 prep: apply namespaces
set -e

INTERNAL_IP="10.0.1.3"       # control-plane private IP
POD_CIDR="192.168.0.0/16"    # Calico default; must not overlap with subnet (10.0.1.0/24)
CALICO_VERSION="v3.28.0"

# conntrack — required by kubeadm preflight, not installed by default on Ubuntu
sudo apt-get install -y -qq conntrack

# kubeadm init — generates TLS certs for all internal components, writes kubeconfig,
# starts control plane as static pods, prints worker join command
sudo kubeadm init \
  --pod-network-cidr=${POD_CIDR} \
  --apiserver-advertise-address=${INTERNAL_IP}

# configure kubectl for current user
mkdir -p ~/.kube
sudo cp /etc/kubernetes/admin.conf ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config

# install Calico CNI — assigns pod IPs from pod CIDR, programs kernel routes
# pod IPs (192.168.x.x) are virtual, GCP only sees VM-to-VM (10.0.1.x) traffic
kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/${CALICO_VERSION}/manifests/calico.yaml

# wait for calico to be ready
kubectl wait --for=condition=Ready pod -l k8s-app=calico-node -n kube-system --timeout=300s

# CRITICAL: GCP blocks IPIP (protocol 4). Switch Calico to VXLAN (UDP 4789)
# which is allowed by GCP's default-allow-internal firewall rule.
# Must patch BEFORE deploying any workloads.
kubectl patch ippool default-ipv4-ippool \
  --type=merge -p '{"spec":{"ipipMode":"Never","vxlanMode":"Always"}}'
kubectl rollout restart daemonset/calico-node -n kube-system
kubectl rollout status daemonset/calico-node -n kube-system --timeout=120s

# install helm — all helm/kubectl commands run on control-plane (API server TLS cert
# only covers internal IP 10.0.1.3, not public IP)
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# create application namespaces
kubectl apply -f ~/anomaly-detector/k8s/namespaces.yaml

echo "control-plane init done"
echo ""
echo "=== WORKER JOIN COMMAND ==="
kubeadm token create --print-join-command
echo "==========================="
echo "Save the above join command — paste into worker-join.sh"
