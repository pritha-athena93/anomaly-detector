# Anomaly Detector — AWS EKS Setup Guide

Production CloudTrail anomaly detection. IsolationForest inference via KServe, LangGraph agent with RAG (pgvector), alerts to Slack + PagerDuty. Fully GitOps via ArgoCD.

**Runtime flow:**
1. CloudTrail → S3 → SQS → poller CronJob → KServe (IsolationForest)
2. Flagged anomalies → Redpanda Kafka topic `anomalies-flagged`
3. LangGraph agent consumes Kafka → RAG (pgvector on RDS) → Bedrock Claude → classify
4. Decision → Slack `#anomalies` + PagerDuty + Postgres audit log

**Training flow (monthly KFP pipeline):**
1. Fetch CloudTrail logs from S3 → feature engineering → Katib HPO
2. Best IsolationForest → S3 model registry → KServe rollout
3. RAG re-index → pgvector → SSM `last_train_ts` updated (only if all steps pass)

---

## Prerequisites

Install these on your local machine before starting:

| Tool | Version | Purpose |
|------|---------|---------|
| Terraform | >= 1.10 | AWS infra provisioning (native S3 locking) |
| AWS CLI | >= 2.x | Auth + Secrets Manager |
| kubectl | >= 1.28 | K8s cluster access (via bastion) |
| helm | >= 3.14 | ArgoCD bootstrap |
| jq | any | JSON parsing in scripts |
| Docker | any | Build container images |
| Python | 3.11+ | Local test runs |

AWS account requirements:
- IAM user/role with permissions: `AdministratorAccess` (or see [Required IAM Permissions](#required-iam-permissions))
- Bedrock model access enabled: `us-east-1` → `claude-3-5-sonnet-20241022`, `amazon.titan-embed-text-v2:0`
- PagerDuty integration key + Slack webhook URL

---

## Step 1 — Configure Terraform Variables

```bash
cd infra/
cp config/terraform.tfvars.example config/terraform.tfvars
```

Edit `infra/config/terraform.tfvars`:

```hcl
region           = "us-east-1"        # must match Bedrock model region
cluster_name     = "anomaly-detector"  # prefix for all AWS resource names
bastion_key_pair = "bastion-key"       # EC2 key pair name (create in step 2)
```

---

## Step 2 — Create EC2 Key Pair for Bastion

```bash
aws ec2 create-key-pair \
  --key-name bastion-key \
  --query 'KeyMaterial' \
  --output text > ~/.ssh/bastion-key.pem

chmod 400 ~/.ssh/bastion-key.pem
```

---

## Step 2.5 — Create Terraform State Backend

Run **once** before the first `terraform init`. Creates the S3 bucket and DynamoDB lock table that `infra/main.tf` uses as its remote backend.

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1

aws s3api create-bucket \
  --bucket "anomaly-detector-tf-state-${ACCOUNT_ID}" \
  --region $REGION

aws s3api put-bucket-versioning \
  --bucket "anomaly-detector-tf-state-${ACCOUNT_ID}" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "anomaly-detector-tf-state-${ACCOUNT_ID}" \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

aws s3api put-public-access-block \
  --bucket "anomaly-detector-tf-state-${ACCOUNT_ID}" \
  --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

Then initialize Terraform with the S3 backend:

```bash
cd infra/

# If backend_override.tf exists (local state fallback), remove it first
rm -f backend_override.tf

terraform init -var-file=config/terraform.tfvars
# If migrating existing local state: terraform init -migrate-state -var-file=config/terraform.tfvars
```

---

## Step 3 — Provision AWS Infrastructure

```bash
cd infra/
terraform init
terraform plan -var-file=config/terraform.tfvars
terraform apply -var-file=config/terraform.tfvars --auto-approve
```

Terraform creates:
- VPC (2 public + 2 private subnets, NAT GW)
- EKS cluster (private endpoint, managed node group `t3.xlarge x3`)
- S3 buckets: `cloudtrail-logs`, `model-registry`, `kfp-artifacts`
- SQS queue + DLQ (redrive after 3 failures, 14-day retention)
- RDS Postgres 16 (`db.t3.medium`, pgvector parameter group, private subnet)
- Secrets Manager: `anomaly/rds-password`, `anomaly/pagerduty-key`, `anomaly/slack-webhook`
- SSM parameters: `/anomaly/last_train_ts`, `/anomaly/model_version`, `/anomaly/threshold`
- 4 IRSA roles: `kserve-sa`, `poller-sa`, `agent-sa`, `kfp-pipeline-sa`
- Bastion EC2 (`t3.micro`, public subnet, kubectl + helm pre-installed)

Save outputs — you'll need them later:

```bash
terraform output -json > ../infra-outputs.json
```

---

## Step 4 — Populate Secrets Manager

Terraform creates the secret entries but leaves the values empty. Fill them:

```bash
# RDS DSN (get endpoint from terraform output)
RDS_ENDPOINT=$(terraform output -raw rds_endpoint)
aws secretsmanager put-secret-value \
  --secret-id anomaly/rds-password \
  --secret-string "{\"dsn\":\"postgresql://anomaly:CHANGE_ME@${RDS_ENDPOINT}:5432/anomaly\"}"

# PagerDuty Events API v2 integration key
aws secretsmanager put-secret-value \
  --secret-id anomaly/pagerduty-key \
  --secret-string '{"key":"YOUR_PD_INTEGRATION_KEY"}'

# Slack webhook URL
aws secretsmanager put-secret-value \
  --secret-id anomaly/slack-webhook \
  --secret-string '{"url":"<webhook-url>'

# Anthropic API key (for Bedrock fallback or direct API use)
aws secretsmanager create-secret \
  --name anomaly/anthropic-key \
  --secret-string '{"key":"sk-ant-YOUR_KEY"}'
```

---

## Step 5 — Set Up RDS Schema

Run from the bastion (RDS is not publicly accessible):

```bash
BASTION_IP=$(cd infra && terraform output -raw bastion_ssh | awk '{print $NF}' | cut -d@ -f2)
scp -i ~/.ssh/bastion-key.pem scripts/setup-rds-schema.sh ec2-user@${BASTION_IP}:~
ssh -i ~/.ssh/bastion-key.pem ec2-user@${BASTION_IP}

# On the bastion:
export RDS_DSN=$(aws secretsmanager get-secret-value \
  --secret-id anomaly/rds-password \
  --query 'SecretString' --output text | jq -r .dsn)
bash setup-rds-schema.sh
```

Creates:
- `rag_chunks` table — pgvector 1536-dim embeddings for RAG retrieval
- `agent_log` table — audit trail of every agent decision (unique `event_id` for idempotency)

---

## Step 6 — Push Repo to GitHub

All K8s resources are GitOps-managed. ArgoCD needs to pull from your repo.

```bash
# In repo root
git remote set-url origin https://github.com/YOUR_ORG/anomaly-detector.git
git push -u origin main
```

> If the repo is private, create a GitHub deploy key and configure it in ArgoCD after step 7.

---

## Step 7 — Bootstrap ArgoCD on the Bastion

SSH to bastion, clone repo, run bootstrap:

```bash
BASTION_IP=$(cd infra && terraform output -raw bastion_ssh | awk '{print $NF}' | cut -d@ -f2)
ssh -i ~/.ssh/bastion-key.pem ec2-user@${BASTION_IP}

# On the bastion:
git clone https://github.com/YOUR_ORG/anomaly-detector.git
cd anomaly-detector
bash scripts/bootstrap-aws.sh anomaly-detector us-east-1 https://github.com/YOUR_ORG/anomaly-detector.git
```

The script:
1. Runs `aws eks update-kubeconfig`
2. Installs ArgoCD via Helm into `argocd` namespace
3. Applies `k8s/argocd/root-app.yaml` (App of Apps)

ArgoCD then auto-syncs in sync-wave order:
1. `namespaces` — creates all namespaces (`vault`, `kafka`, `cert-manager`, `reflector`, `ml-agent`, `anomaly-poller`, `kserve`, `kubeflow`, `monitoring`)
2. `cert-manager` + cluster issuers — internal CA + selfsigned issuer
3. `reflector` — copies certs across namespaces automatically
4. `vault` — HA Raft 3-replica with TLS
5. `redpanda-operator` → `redpanda-cluster` — Kafka with TLS
6. `kube-prometheus-stack` — Grafana + Prometheus

Monitor sync:
```bash
kubectl get applications -n argocd
# or port-forward ArgoCD UI:
kubectl port-forward svc/argocd-server -n argocd 8080:443
# UI: https://localhost:8080
# Password: kubectl get secret argocd-initial-admin-secret -n argocd -o jsonpath='{.data.password}' | base64 -d
```

---

## Step 8 — Initialize and Unseal Vault

**Auto-unseal:** Vault uses AWS KMS (`alias/anomaly-detector-vault-unseal`) for auto-unseal.
Pods auto-unseal on restart — no manual intervention needed after first-time init.

### 8a — First-time Init (run once)

```bash
# Init on vault-0 (5 keys, threshold 3). Store output OFFLINE — never commit.
kubectl exec -n vault vault-0 -- sh -c \
  'VAULT_ADDR=https://127.0.0.1:8200 VAULT_SKIP_VERIFY=true vault operator init -key-shares=5 -key-threshold=3 -format=json'
```

Save the 5 unseal keys and root token to a secure offline location (password manager, printed paper, HSM).

### 8b — One-time Seal Migration (Shamir → KMS)

Required only once after init. After this, pods auto-unseal forever.

```bash
# Unseal all 3 nodes with -migrate flag (uses Shamir keys to re-encrypt under KMS)
K1=<key1> K2=<key2> K3=<key3>
for pod in vault-0 vault-1 vault-2; do
  for k in $K1 $K2 $K3; do
    kubectl exec -n vault $pod -- sh -c \
      "VAULT_ADDR=https://127.0.0.1:8200 VAULT_SKIP_VERIFY=true vault operator unseal -migrate $k"
  done
done

# Verify: Seal Type should be awskms, Sealed=false
kubectl exec -n vault vault-0 -- sh -c \
  'VAULT_ADDR=https://127.0.0.1:8200 VAULT_SKIP_VERIFY=true vault status' | grep -E 'Seal Type|Sealed'
```

After migration, Vault logs show `unsealed with stored key` on every restart — fully automatic.

### 8c — Configure Vault (run once after init)

```bash
# Use active vault node (find with: vault status | grep "HA Mode.*active")
ACTIVE_POD=vault-2  # or whichever shows active
ROOT_TOKEN=<root-token-from-init>

kubectl exec -n vault $ACTIVE_POD -- sh -c "
export VAULT_ADDR=https://127.0.0.1:8200 VAULT_SKIP_VERIFY=true VAULT_TOKEN=$ROOT_TOKEN

# Enable KV v2
vault secrets enable -path=secret kv-v2

# Write secrets from AWS Secrets Manager
vault kv put secret/anomaly/rds \
  dsn=\"\$(aws secretsmanager get-secret-value --secret-id anomaly/rds-password --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"dsn\"])')\"

vault kv put secret/anomaly/pagerduty \
  key=\"\$(aws secretsmanager get-secret-value --secret-id anomaly/pagerduty-key --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"key\"])')\"

vault kv put secret/anomaly/slack \
  webhook_url=\"\$(aws secretsmanager get-secret-value --secret-id anomaly/slack-webhook --query SecretString --output text | python3 -c 'import json,sys; print(json.load(sys.stdin)[\"url\"])')\"

# Enable Kubernetes auth
vault auth enable kubernetes
vault write auth/kubernetes/config kubernetes_host=https://kubernetes.default.svc:443

# Policies (match k8s/helm/vault/policies/*.hcl)
vault policy write agent-policy - <<'EOF'
path \"secret/data/anomaly/rds\"        { capabilities = [\"read\"] }
path \"secret/data/anomaly/slack\"      { capabilities = [\"read\"] }
path \"secret/data/anomaly/pagerduty\"  { capabilities = [\"read\"] }
path \"secret/data/anomaly/kafka\"      { capabilities = [\"read\"] }
path \"auth/token/renew-self\"          { capabilities = [\"update\"] }
path \"auth/token/lookup-self\"         { capabilities = [\"read\"] }
EOF

vault policy write poller-policy - <<'EOF'
path \"secret/data/anomaly/kafka\"      { capabilities = [\"read\"] }
path \"auth/token/renew-self\"          { capabilities = [\"update\"] }
path \"auth/token/lookup-self\"         { capabilities = [\"read\"] }
EOF

vault policy write training-policy - <<'EOF'
path \"secret/data/anomaly/rds\"        { capabilities = [\"read\"] }
path \"auth/token/renew-self\"          { capabilities = [\"update\"] }
path \"auth/token/lookup-self\"         { capabilities = [\"read\"] }
EOF

# K8s auth roles
vault write auth/kubernetes/role/ml-agent \
  bound_service_account_names=agent-sa bound_service_account_namespaces=ml-agent \
  policies=agent-policy ttl=1h

vault write auth/kubernetes/role/anomaly-poller \
  bound_service_account_names=poller-sa bound_service_account_namespaces=anomaly-poller \
  policies=poller-policy ttl=1h

vault write auth/kubernetes/role/training-pipeline \
  bound_service_account_names=training-sa bound_service_account_namespaces=training-pipeline \
  policies=training-policy ttl=1h
"
```

### 8d — After Redpanda is Running

Write Kafka broker addresses to Vault (needed by agent + poller):

```bash
kubectl exec -n vault $ACTIVE_POD -- sh -c "
export VAULT_ADDR=https://127.0.0.1:8200 VAULT_SKIP_VERIFY=true VAULT_TOKEN=$ROOT_TOKEN
vault kv put secret/anomaly/kafka \
  brokers='anomaly-redpanda-0.anomaly-redpanda.kafka.svc:9093'
"
```

---

## Step 9 — Build and Push Container Images

ECR repos: `anomaly-detector/agent`, `anomaly-detector/poller`, `anomaly-detector/training-pipeline`

```bash
ECR_REGISTRY=984445750473.dkr.ecr.us-east-1.amazonaws.com
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $ECR_REGISTRY

# Create ECR repos (first time only)
for repo in anomaly-detector/agent anomaly-detector/poller anomaly-detector/training-pipeline; do
  aws ecr create-repository --repository-name $repo --region us-east-1
done

# Build + push agent
docker build -t $ECR_REGISTRY/anomaly-detector/agent:latest -f agent/Dockerfile .
docker push $ECR_REGISTRY/anomaly-detector/agent:latest

# Build + push poller
docker build -t $ECR_REGISTRY/anomaly-detector/poller:latest -f poller/Dockerfile .
docker push $ECR_REGISTRY/anomaly-detector/poller:latest

# Build + push training-pipeline
docker build -t $ECR_REGISTRY/anomaly-detector/training-pipeline:latest -f ml/Dockerfile .
docker push $ECR_REGISTRY/anomaly-detector/training-pipeline:latest
```

Image refs in manifests are already set to the above URLs. ArgoCD auto-syncs on push.

---

## Step 10 — Deploy Kubeflow Pipelines

Kubeflow deploys via ArgoCD (`k8s/argocd/apps/kubeflow.yaml`, source: `kubeflow/manifests` v1.9.1).
No manual `kubectl apply` needed — ArgoCD syncs it automatically once root app is applied.

Monitor from bastion:

```bash
# Watch kubeflow app sync
kubectl get application kubeflow -n argocd -w

# Wait for all deployments ready (~10 min)
kubectl wait --for=condition=Available deployment --all -n kubeflow --timeout=600s
```

---

## Step 11 — Run Initial Training Pipeline

Upload initial training data to S3:

```bash
MODEL_REGISTRY_BUCKET=$(cd infra && terraform output -raw model_registry_bucket)
CLOUDTRAIL_BUCKET=$(cd infra && terraform output -raw cloudtrail_logs_bucket)

# Upload your CloudTrail parquet or let the pipeline fetch from S3 directly
aws s3 cp /path/to/features.parquet s3://${MODEL_REGISTRY_BUCKET}/data/processed/features.parquet
```

Submit the KFP pipeline:

```bash
pip install kfp==2.9.0
# Port-forward KFP UI
kubectl port-forward svc/ml-pipeline-ui -n kubeflow 8888:80

# Compile + submit
python ml/training_pipeline.py  # compiles to training_pipeline.yaml
# Upload training_pipeline.yaml via KFP UI at http://localhost:8888
# Or use kfp client:
python - <<EOF
from kfp import Client
client = Client(host='http://localhost:8888')
client.create_run_from_pipeline_package(
    'training_pipeline.yaml',
    arguments={
        'cloudtrail_bucket': '${CLOUDTRAIL_BUCKET}',
        'model_registry_bucket': '${MODEL_REGISTRY_BUCKET}',
        'model_version': 'v1',
    }
)
EOF
```

Pipeline steps (in order, each step blocks the next):
1. `fetch_logs` — pull CloudTrail from S3
2. `engineer_features` — build parquet with 18 feature columns
3. `run_katib_hpo` — tune `n_estimators`, `contamination`, `max_samples`
4. `train_and_evaluate` — best trial → IsolationForest → MLflow
5. `register_model` — promote to S3 `latest/`
6. `update_kserve` — patch InferenceService `storageUri`
7. `index_rag_chunks` — embed infra changes → pgvector
8. `write_train_ts` — update SSM `/anomaly/last_train_ts` (only if all above pass)

---

## Step 12 — Deploy KServe InferenceService

KServe operator + InferenceService deploy via ArgoCD (`k8s/argocd/apps/kserve.yaml`, sync-wave 3/4).
`k8s/apps/kserve/inference-service.yaml` is auto-synced — no manual apply.

Monitor:

```bash
kubectl get inferenceservice -n kserve
# Wait for READY=True
kubectl get application kserve -n argocd
```

---

## Step 13 — Deploy Agent and Poller

Agent and poller deploy via ArgoCD (`k8s/argocd/apps/ml-agent.yaml`, `k8s/argocd/apps/anomaly-poller.yaml`, sync-wave 4).
Manifests: `k8s/apps/ml-agent/`, `k8s/apps/anomaly-poller/` — auto-synced, no manual apply.

Verify:

```bash
kubectl get pods -n ml-agent
kubectl get cronjob -n anomaly-poller
```

---

## Step 14 — Configure CloudTrail → S3 → SQS

Enable CloudTrail delivery to S3 and wire up EventBridge → SQS:

```bash
# CloudTrail should already deliver to the S3 bucket from terraform output
CLOUDTRAIL_BUCKET=$(cd infra && terraform output -raw cloudtrail_logs_bucket)

# Verify S3 notification to SQS is configured (set in main.tf)
SQS_URL=$(cd infra && terraform output -raw sqs_queue_url)
echo "Poller SQS_URL: $SQS_URL"

# Configure env var in poller CronJob:
kubectl set env cronjob/anomaly-poller -n anomaly-poller \
  SQS_URL=$SQS_URL \
  KSERVE_URL=http://anomaly-detector.kserve.svc/v2/models/anomaly-detector:infer \
  KAFKA_BROKERS=anomaly-redpanda-0.anomaly-redpanda.kafka.svc:9093 \
  MODEL_REGISTRY_BUCKET=$(cd infra && terraform output -raw model_registry_bucket) \
  THRESHOLD=-0.1
```

---

## Step 15 — Verify End-to-End

```bash
# 1. Trigger a test CloudTrail event (e.g. list S3 buckets — generates a read event)
aws s3 ls

# 2. Check poller CronJob runs (fires every 5 min)
kubectl get jobs -n anomaly-poller

# 3. Check Kafka topics have messages
kubectl exec -n kafka anomaly-redpanda-0 -- \
  rpk topic consume events-raw --num 5

# 4. Check agent is processing
kubectl logs -n ml-agent -l app=ml-agent -f

# 5. Check agent_log in Postgres
kubectl exec -n vault vault-0 -- vault kv get secret/anomaly/rds
# Then: psql $RDS_DSN -c "SELECT event_id, decision, confidence FROM agent_log LIMIT 5;"

# 6. Check Grafana dashboards
kubectl port-forward svc/kube-prometheus-stack-grafana -n monitoring 3000:80
# Login: admin / (get from Vault or Grafana secret)
```

---

## Architecture Reference

```
CloudTrail ──► S3 (cloudtrail-logs/)
                    │
            EventBridge (s3:ObjectCreated)
                    │
               SQS Queue ──► DLQ (after 3 failures)
                    │
   ┌────────────────▼────────────────────────────────────┐
   │  EKS Cluster (private)                               │
   │                                                      │
   │  anomaly-poller (CronJob, 5min)                      │
   │    └─► KServe/IsolationForest ──► Kafka              │
   │                                    │                  │
   │  ml-agent (Deployment, 2 replicas) │                  │
   │    └─► Kafka consumer              │                  │
   │         └─► idempotency check ─────┘                  │
   │              └─► RAG (pgvector/RDS)                  │
   │                   └─► Bedrock Claude (w/ retry)      │
   │                        └─► Slack + PagerDuty         │
   │                             └─► agent_log (RDS)      │
   │                                                      │
   │  ArgoCD    cert-manager    Reflector (cert copy)     │
   │  Vault HA  Redpanda(TLS)  Prometheus+Grafana         │
   └──────────────────────────────────────────────────────┘
                    │
              RDS Postgres 16
              (pgvector, rag_chunks, agent_log)
```

---

## Key Configuration Files

| File | Purpose |
|------|---------|
| `infra/main.tf` | All AWS resources (VPC, EKS, RDS, SQS, S3, IAM) |
| `infra/variables.tf` | Terraform input variables |
| `infra/config/terraform.tfvars` | Your environment values (gitignored) |
| `k8s/argocd/root-app.yaml` | ArgoCD App of Apps entry point |
| `k8s/argocd/apps/` | ArgoCD Applications for each component |
| `k8s/namespaces.yaml` | All K8s namespaces (managed by ArgoCD) |
| `k8s/manifests/cert-manager/cluster-issuer.yaml` | Internal CA + Reflector annotations |
| `k8s/helm/vault/values.yaml` | Vault HA Raft config with TLS |
| `k8s/manifests/kafka/cluster.yaml` | Redpanda 3-replica cluster with TLS |
| `k8s/manifests/kafka/topics.yaml` | `events-raw` + `anomalies-flagged` topics |
| `k8s/manifests/kserve/inference-service.yaml` | IsolationForest serving |
| `k8s/manifests/ml-agent/deployment.yaml` | LangGraph agent Deployment |
| `agent/graph.py` | LangGraph pipeline (idempotency + Bedrock retry) |
| `ml/training_pipeline.py` | KFP v2 pipeline (8 steps, strict ordering) |
| `ml/trainer.py` | Katib trial + feature schema export |
| `poller/poller.py` | SQS → KServe → Kafka (schema validation at startup) |
| `scripts/bootstrap-aws.sh` | One-time bastion bootstrap |
| `scripts/setup-rds-schema.sh` | Postgres schema init |

---

## Required IAM Permissions

Terraform apply needs these AWS permissions:

```
ec2:*, eks:*, iam:*, s3:*, sqs:*, rds:*, secretsmanager:*, ssm:*, sts:GetCallerIdentity
```

Narrow to least-privilege after first apply using `infra-outputs.json` to scope resource ARNs.

---

## Troubleshooting

**ArgoCD app stuck Progressing:**
```bash
kubectl describe application <app-name> -n argocd
```

**Vault sealed after pod restart:**
```bash
kubectl exec -n vault vault-0 -- vault status
# If sealed=true:
kubectl exec -n vault vault-0 -- vault operator unseal KEY1
kubectl exec -n vault vault-0 -- vault operator unseal KEY2
kubectl exec -n vault vault-0 -- vault operator unseal KEY3
```

**Redpanda TLS errors:**
```bash
# Check cert is propagated to kafka namespace via Reflector
kubectl get secret anomaly-redpanda-default-root-certificate -n kafka
```

**Poller exits non-zero on startup:**
```bash
kubectl logs job/<poller-job> -n anomaly-poller
# "Feature schema drift detected" → retrain model or update FEATURE_COLS in poller.py
```

**Agent duplicate Slack messages:**
Agent has idempotency check at startup via `agent_log.event_id UNIQUE`. If duplicate messages appear, check that `agent_log` table has the `UNIQUE` constraint (`\d agent_log` in psql).

**KServe InferenceService not Ready:**
```bash
kubectl describe inferenceservice anomaly-detector -n kserve
# Common: wrong S3 path, IRSA role missing S3 permissions, model not yet in S3
```

**SQS DLQ has messages:**
```bash
SQS_DLQ_URL=$(cd infra && terraform output -raw sqs_dlq_url)
aws sqs receive-message --queue-url $SQS_DLQ_URL
# Malformed S3 event bodies or S3 permission errors in the poller
```
