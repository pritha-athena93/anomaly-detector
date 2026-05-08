#!/bin/bash
# Run on ALL nodes (control-plane, worker-1, worker-2)
# Phase 1, Steps 4-7: swap disable, kernel modules, sysctl
set -e

# Step 4: disable swap — k8s can't enforce memory limits if swap is available
sudo swapoff -a
sudo sed -i '/\bswap\b/d' /etc/fstab

# Step 5 & 6: load overlay + br_netfilter, persist across reboots
sudo modprobe overlay
sudo modprobe br_netfilter

cat <<EOF | sudo tee /etc/modules-load.d/k8s.conf
overlay
br_netfilter
EOF

# Step 7: sysctl — bridge traffic hits iptables, node can route pod traffic
cat <<EOF | sudo tee /etc/sysctl.d/k8s.conf
net.bridge.bridge-nf-call-iptables  = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward                 = 1
EOF
sudo sysctl --system

echo "node-prep done: $(hostname)"
