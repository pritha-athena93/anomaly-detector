# ── Cluster is PRIVATE (endpoint_public_access = false) ──
#
# Terraform runs outside the VPC and cannot reach the K8s API.
# The helm and kubernetes providers are therefore NOT configured here.
#
# All Kubernetes resources (namespaces, Helm releases, manifests) are
# managed by ArgoCD, which runs inside the cluster.
#
# Bootstrap sequence (run once from the bastion — see scripts/bootstrap-aws.sh):
#   1. aws eks update-kubeconfig ...         ← configure kubectl on bastion
#   2. helm install argocd ...               ← seed ArgoCD itself
#   3. kubectl apply -f k8s/argocd/root-app.yaml ← hand control to ArgoCD
#
# After step 3, ArgoCD pulls from Git and self-manages everything:
#   namespaces          → k8s/namespaces.yaml
#   cert-manager        → k8s/argocd/apps/cert-manager.yaml
#   reflector           → k8s/argocd/apps/reflector.yaml
#   redpanda            → k8s/argocd/apps/redpanda.yaml
#   vault               → k8s/argocd/apps/vault.yaml
#   kube-prometheus-stack → k8s/argocd/apps/kube-prometheus-stack.yaml
