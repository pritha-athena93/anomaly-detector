# vault-extras

Helm chart for cert-manager Certificate and supporting resources for Vault HA TLS.

## What it does

Issues the `vault-tls` TLS certificate via cert-manager and configures reflector to copy it to consumer namespaces. Deployed as a separate Helm chart alongside the HashiCorp Vault chart so cert configuration is versioned, templated, and ArgoCD-managed.

## How it works

- `templates/certificate.yaml` — cert-manager `Certificate` resource issued by `internal-ca-issuer` ClusterIssuer
- Certificate SANs include both the Vault service DNS names AND the pod-level headless service names (`vault-N.vault-internal`) required for Raft inter-pod TLS
- Reflector annotations on the secret cause the `vault-tls` secret to be automatically copied to consumer namespaces

## Key decisions

**Why separate chart?** The HashiCorp vault Helm chart is pulled from `helm.releases.hashicorp.com`. The certificate must be applied alongside it but is not part of that chart. A dedicated chart lets us use Helm templating (values-driven SANs, namespace list) instead of a raw YAML manifest.

**Why Raft SANs?** Vault HA Raft uses `vault-N.vault-internal:8201` for peer-to-peer TLS. Without those SANs the certificate fails x509 validation and pods CrashLoopBackOff with `tls: failed to verify certificate`.

## How to use

ArgoCD applies this chart as the third source in the vault Application ([k8s/argocd/apps/vault.yaml](../../../argocd/apps/vault.yaml)).

To change reflected namespaces, edit `values.yaml`:

```yaml
certificate:
  reflectionNamespaces: "ml-agent,anomaly-poller,training-pipeline,kubeflow,monitoring"
```

To add SANs (e.g. for a new Vault replica):

```yaml
certificate:
  dnsNames:
    - vault-3.vault-internal
```

## Dependencies

- cert-manager with `internal-ca-issuer` ClusterIssuer (sync-wave 1, always before vault at wave 3)
- Reflector controller deployed in `reflector` namespace (sync-wave 2)
