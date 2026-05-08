#!/bin/bash
# Run on ALL nodes (control-plane, worker-1, worker-2)
# Phase 1, Step 8-9: install containerd, enable SystemdCgroup
set -e

sudo apt-get update -qq
sudo apt-get install -y -qq ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | sudo tee /etc/apt/sources.list.d/docker.list

sudo apt-get update -qq
sudo apt-get install -y -qq containerd.io

sudo mkdir -p /etc/containerd
containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
# SystemdCgroup=true — hands cgroup management to systemd, prevents two competing cgroup managers
sudo sed -i "s/SystemdCgroup = false/SystemdCgroup = true/" /etc/containerd/config.toml

sudo systemctl restart containerd
sudo systemctl enable containerd

echo "containerd done: $(hostname) | $(grep SystemdCgroup /etc/containerd/config.toml | tr -d ' ')"
