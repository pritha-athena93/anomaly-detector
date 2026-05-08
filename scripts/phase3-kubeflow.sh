#!/bin/bash
# Run on control-plane
# Phase 3: cert-manager v1.14.5, kustomize v5.8.1, Kubeflow v1.9.1
# Install runs as background nohup process — takes ~15 min
# Monitor: tail -f /tmp/kubeflow-install.log
set -e

CERT_MANAGER_VERSION="v1.14.5"
KUSTOMIZE_VERSION="5.8.1"
KUBEFLOW_VERSION="v1.9.1"

# cert-manager — Kubeflow requires it for webhook TLS certs
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml
kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=300s

# kustomize — Kubeflow uses kustomize (not Helm) for install
curl -s "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh" | bash -s ${KUSTOMIZE_VERSION}
sudo mv kustomize /usr/local/bin/

# clone kubeflow manifests
git clone https://github.com/kubeflow/manifests.git ~/manifests
cd ~/manifests && git checkout ${KUBEFLOW_VERSION}

# apply kubeflow — retry loop required: CRDs take time to register before dependent
# resources can be created. Each failed apply is expected until CRDs are fully ready.
cd ~/manifests
nohup bash -c '
  while ! kustomize build example | kubectl apply -f -; do
    echo "$(date): apply failed, retrying in 20s..."
    sleep 20
  done
  echo "$(date): kubeflow apply complete"
' > /tmp/kubeflow-install.log 2>&1 &

echo "Kubeflow install running in background PID: $!"
echo "Monitor: tail -f /tmp/kubeflow-install.log"
echo ""
echo "After complete, verify:"
echo "  kubectl get pods -n kubeflow"
echo "  kubectl wait --for=condition=Ready pod --all -n kubeflow --timeout=600s"
echo "  kubectl port-forward svc/istio-ingressgateway -n istio-system 8080:80"
echo "  Login: user@example.com / 12341234"
