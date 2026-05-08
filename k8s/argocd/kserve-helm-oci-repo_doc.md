# kserve-helm-oci-repo.yaml

## What it does
Registers `ghcr.io/kserve/charts` as an OCI Helm repository in ArgoCD.

## How it works
ArgoCD requires explicit repository registration for OCI Helm registries. This Secret (labeled `argocd.argoproj.io/secret-type: repository`) tells ArgoCD to treat `ghcr.io/kserve/charts` as a Helm repo with OCI enabled. No credentials needed — registry is public.

The KServe Helm charts moved from `https://kserve.github.io/helm-charts` (404 as of 2026) to OCI at `ghcr.io/kserve/charts`.

## How to use
Applied manually from bastion (one-time, before ArgoCD syncs kserve app):
```bash
kubectl apply -f k8s/argocd/kserve-helm-oci-repo.yaml
```
Or commit to repo — root ArgoCD app auto-syncs it.

## Dependencies
- ArgoCD running in `argocd` namespace
- `k8s/argocd/apps/kserve.yaml` updated to use `repoURL: ghcr.io/kserve/charts`
