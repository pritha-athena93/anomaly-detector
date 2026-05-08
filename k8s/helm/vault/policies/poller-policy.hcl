# Vault policy for poller-sa ServiceAccount (namespace: anomaly-poller)
# Poller needs kafka brokers (for publishing) — SQS/S3 access is via IRSA, not Vault.

path "secret/data/anomaly/kafka"      { capabilities = ["read"] }

path "auth/token/renew-self"          { capabilities = ["update"] }
path "auth/token/lookup-self"         { capabilities = ["read"] }
