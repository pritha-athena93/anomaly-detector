# Vault policy for training-sa ServiceAccount (namespace: training-pipeline)
# Training pipeline needs Postgres DSN for rag_indexer upserts.
# S3/SSM/Bedrock access is via IRSA (kfp-pipeline-sa role).

path "secret/data/anomaly/postgres"   { capabilities = ["read"] }

path "auth/token/renew-self"          { capabilities = ["update"] }
path "auth/token/lookup-self"         { capabilities = ["read"] }
