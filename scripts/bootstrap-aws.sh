#!/usr/bin/env bash
# bootstrap-aws.sh — run once from the bastion after `terraform apply`.
#
# What this does:
#   1. Configures kubectl against the private EKS cluster
#   2. Installs ArgoCD via Helm (the one component that can't bootstrap itself)
#   3. Applies the root App of Apps — ArgoCD takes over from here
#
# Prerequisites on the bastion:
#   aws CLI configured (via instance profile — no static keys needed)
#   helm, kubectl installed (user_data in bastion.tf handles this)
#
# Run as:
#   bash scripts/bootstrap-aws.sh <cluster-name> <region> <repo-url>
#
# Example:
#   bash scripts/bootstrap-aws.sh anomaly-detector us-east-1 https://github.com/myorg/anomaly-detector.git

set -euo pipefail

CLUSTER_NAME="${1:?usage: $0 <cluster-name> <region> <repo-url>}"
REGION="${2:?usage: $0 <cluster-name> <region> <repo-url>}"
REPO_URL="${3:?usage: $0 <cluster-name> <region> <repo-url>}"

echo "==> Configuring kubectl"
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME"

echo "==> Waiting for cluster API"
kubectl wait --for=condition=Ready nodes --all --timeout=300s

echo "==> Creating argocd namespace"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -

echo "==> Installing ArgoCD"
helm repo add argo https://argoproj.github.io/argo-helm
helm repo update

helm upgrade --install argocd argo/argo-cd \
  --namespace argocd \
  --version 7.3.4 \
  --set server.extraArgs[0]="--insecure" \
  --wait --timeout 300s

echo "==> Waiting for ArgoCD server"
kubectl wait deployment argocd-server -n argocd --for=condition=Available --timeout=120s

echo "==> Patching root-app.yaml with actual repo URL"
# Inline-substitute the placeholder before applying
sed "s|https://github.com/pritha-athena93/anomaly-detector.git|${REPO_URL}|g" \
  k8s/argocd/root-app.yaml | kubectl apply -f -

echo ""
echo "==> Done. ArgoCD is now managing the cluster."
echo "    ArgoCD UI: kubectl port-forward svc/argocd-server -n argocd 8080:443"
echo "    Initial admin password: kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d"
echo ""
echo "==> ArgoCD will now sync (in order):"
echo "    1. namespaces"
echo "    2. cert-manager + cluster-issuers"
echo "    3. reflector"
echo "    4. vault (TLS via cert-manager)"
echo "    5. redpanda-operator → redpanda-cluster"
echo "    6. kube-prometheus-stack"
echo ""
echo "==> After Vault syncs, initialise and unseal it:"
echo "    kubectl exec -n vault vault-0 -- vault operator init"
echo "    kubectl exec -n vault vault-0 -- vault operator unseal <key1>"
echo "    kubectl exec -n vault vault-0 -- vault operator unseal <key2>"
echo "    kubectl exec -n vault vault-0 -- vault operator unseal <key3>"
echo "    Store the root token and unseal keys securely offline."
