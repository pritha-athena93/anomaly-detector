# Vault policy for ml-agent ServiceAccount (namespace: ml-agent)
# Grants read-only access to anomaly secrets needed by the LangGraph agent.
# Applied via: vault policy write agent-policy k8s/manifests/vault/policies/agent-policy.hcl

path "secret/data/anomaly/rds"        { capabilities = ["read"] }
path "secret/data/anomaly/slack"      { capabilities = ["read"] }
path "secret/data/anomaly/pagerduty"  { capabilities = ["read"] }
path "secret/data/anomaly/kafka"      { capabilities = ["read"] }

# Allow token renewal
path "auth/token/renew-self"          { capabilities = ["update"] }
path "auth/token/lookup-self"         { capabilities = ["read"] }
