#!/usr/bin/env bash
# check_infra.sh — Pre-flight infrastructure health check
#
# Runs fast, non-destructive checks for every acceptance criterion in PRODUCTION_PLAN.md.
# Safe to run at any time — no writes.
#
# Usage:
#   export TEST_CLUSTER_NAME=anomaly-detector
#   export TEST_AWS_REGION=us-east-1
#   export TEST_984445750473=$(aws sts get-caller-identity --query Account --output text)
#   bash scripts/check_infra.sh
#
# Exit code: 0 = all passed, 1 = one or more checks failed.

set -euo pipefail

CLUSTER_NAME="${TEST_CLUSTER_NAME:-anomaly-detector}"
AWS_REGION="${TEST_AWS_REGION:-us-east-1}"
984445750473="${TEST_984445750473:-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "UNKNOWN")}"

TF_STATE_BUCKET="anomaly-detector-tf-state-${984445750473}"
CLOUDTRAIL_BUCKET="${CLUSTER_NAME}-cloudtrail-logs-${984445750473}"
MODEL_REG_BUCKET="${CLUSTER_NAME}-model-registry-${984445750473}"
SQS_QUEUE_NAME="${CLUSTER_NAME}-cloudtrail-events"

PASS=0
FAIL=0
SKIP=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass()  { echo -e "${GREEN}[PASS]${NC} $1"; ((PASS++)); }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; ((FAIL++)); }
skip()  { echo -e "${YELLOW}[SKIP]${NC} $1"; ((SKIP++)); }
header(){ echo -e "\n${YELLOW}=== $1 ===${NC}"; }

require_cmd() {
    if ! command -v "$1" &>/dev/null; then
        skip "$1 not installed — skipping $2 checks"
        return 1
    fi
    return 0
}


# ── PHASE 0: Terraform / AWS Resources ───────────────────────────────────────
header "Phase 0: AWS Resources"

if require_cmd aws "AWS"; then

    # S3 state bucket
    if aws s3api head-bucket --bucket "${TF_STATE_BUCKET}" 2>/dev/null; then
        versioning=$(aws s3api get-bucket-versioning --bucket "${TF_STATE_BUCKET}" \
                     --query "Status" --output text 2>/dev/null)
        [[ "$versioning" == "Enabled" ]] && \
            pass "TF state bucket versioning enabled" || \
            fail "TF state bucket versioning NOT enabled (got: $versioning)"

        encryption=$(aws s3api get-bucket-encryption --bucket "${TF_STATE_BUCKET}" \
                     --query "ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm" \
                     --output text 2>/dev/null)
        [[ "$encryption" == "AES256" ]] && \
            pass "TF state bucket AES256 encrypted" || \
            fail "TF state bucket encryption: $encryption"
    else
        fail "TF state bucket '${TF_STATE_BUCKET}' not found"
    fi

    # DynamoDB lock table
    if aws dynamodb describe-table --table-name anomaly-tf-locks \
       --query "Table.TableStatus" --output text 2>/dev/null | grep -q "ACTIVE"; then
        pass "DynamoDB lock table 'anomaly-tf-locks' active"
    else
        fail "DynamoDB lock table 'anomaly-tf-locks' not found or not ACTIVE"
    fi

    # SQS queue
    if sqs_url=$(aws sqs get-queue-url --queue-name "${SQS_QUEUE_NAME}" \
                 --query "QueueUrl" --output text 2>/dev/null); then
        pass "SQS queue '${SQS_QUEUE_NAME}' exists"

        visibility=$(aws sqs get-queue-attributes --queue-url "${sqs_url}" \
                     --attribute-names VisibilityTimeout \
                     --query "Attributes.VisibilityTimeout" --output text 2>/dev/null)
        [[ "$visibility" == "300" ]] && \
            pass "SQS visibility timeout = 300s" || \
            fail "SQS visibility timeout = ${visibility} (expected 300)"
    else
        fail "SQS queue '${SQS_QUEUE_NAME}' not found"
    fi

    # RDS
    rds_status=$(aws rds describe-db-instances \
                 --db-instance-identifier "${CLUSTER_NAME}-postgres" \
                 --query "DBInstances[0].DBInstanceStatus" \
                 --output text 2>/dev/null || echo "NOT_FOUND")
    [[ "$rds_status" == "available" ]] && \
        pass "RDS instance '${CLUSTER_NAME}-postgres' available" || \
        fail "RDS instance status: ${rds_status}"

    rds_public=$(aws rds describe-db-instances \
                 --db-instance-identifier "${CLUSTER_NAME}-postgres" \
                 --query "DBInstances[0].PubliclyAccessible" \
                 --output text 2>/dev/null || echo "UNKNOWN")
    [[ "$rds_public" == "False" ]] && \
        pass "RDS not publicly accessible" || \
        fail "RDS publicly accessible: ${rds_public}"

    rds_deletion=$(aws rds describe-db-instances \
                   --db-instance-identifier "${CLUSTER_NAME}-postgres" \
                   --query "DBInstances[0].DeletionProtection" \
                   --output text 2>/dev/null || echo "UNKNOWN")
    [[ "$rds_deletion" == "True" ]] && \
        pass "RDS deletion protection enabled" || \
        fail "RDS deletion protection: ${rds_deletion}"

    rds_encrypted=$(aws rds describe-db-instances \
                    --db-instance-identifier "${CLUSTER_NAME}-postgres" \
                    --query "DBInstances[0].StorageEncrypted" \
                    --output text 2>/dev/null || echo "UNKNOWN")
    [[ "$rds_encrypted" == "True" ]] && \
        pass "RDS storage encrypted" || \
        fail "RDS storage encrypted: ${rds_encrypted}"

    # SSM parameters
    for param in "/anomaly/last_train_ts" "/anomaly/model_version" "/anomaly/threshold"; do
        val=$(aws ssm get-parameter --name "$param" \
              --query "Parameter.Value" --output text 2>/dev/null || echo "")
        [[ -n "$val" ]] && \
            pass "SSM parameter $param exists (value: $val)" || \
            fail "SSM parameter $param missing"
    done

    # ECR repos
    for repo in "agent" "training-pipeline" "poller"; do
        repo_name="${CLUSTER_NAME}/${repo}"
        scan=$(aws ecr describe-repositories --repository-names "${repo_name}" \
               --query "repositories[0].imageScanningConfiguration.scanOnPush" \
               --output text 2>/dev/null || echo "NOT_FOUND")
        if [[ "$scan" == "NOT_FOUND" ]]; then
            fail "ECR repo '${repo_name}' not found"
        elif [[ "$scan" == "True" ]]; then
            pass "ECR repo '${repo_name}' scan-on-push enabled"
        else
            fail "ECR repo '${repo_name}' scan-on-push disabled"
        fi
    done

    # S3 public access block
    for bucket in "${TF_STATE_BUCKET}" "${CLOUDTRAIL_BUCKET}" "${MODEL_REG_BUCKET}"; do
        block=$(aws s3api get-public-access-block --bucket "${bucket}" \
                --query "PublicAccessBlockConfiguration.BlockPublicAcls" \
                --output text 2>/dev/null || echo "NOT_FOUND")
        [[ "$block" == "True" ]] && \
            pass "S3 bucket '${bucket}' public access blocked" || \
            fail "S3 bucket '${bucket}' public access NOT blocked (got: $block)"
    done

fi


# ── PHASE 1-2: Kubernetes Resources ──────────────────────────────────────────
header "Phase 1-2: Kubernetes"

if require_cmd kubectl "K8s"; then

    # Namespaces
    for ns in argocd cert-manager vault kafka kubeflow kserve anomaly-poller ml-agent training-pipeline monitoring; do
        if kubectl get namespace "${ns}" &>/dev/null; then
            pass "Namespace '${ns}' exists"
        else
            fail "Namespace '${ns}' missing"
        fi
    done

    # default-deny-all NetworkPolicies
    echo ""
    deny_count=$(kubectl get networkpolicy -A -o json 2>/dev/null | \
        python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for i in d['items'] if i['metadata']['name']=='default-deny-all'))" 2>/dev/null || echo "0")
    [[ "$deny_count" -ge 10 ]] && \
        pass "default-deny-all NetworkPolicy in all 10 namespaces ($deny_count found)" || \
        fail "default-deny-all missing in some namespaces (found $deny_count, expected >=10)"

    # ArgoCD apps
    for app in namespaces cert-manager vault strimzi kserve anomaly-poller ml-agent training-pipeline monitoring; do
        sync=$(kubectl get application "${app}" -n argocd \
               -o jsonpath='{.status.sync.status}' 2>/dev/null || echo "NOT_FOUND")
        health=$(kubectl get application "${app}" -n argocd \
                 -o jsonpath='{.status.health.status}' 2>/dev/null || echo "NOT_FOUND")
        if [[ "$sync" == "Synced" && "$health" == "Healthy" ]]; then
            pass "ArgoCD app '${app}' Synced + Healthy"
        elif [[ "$sync" == "NOT_FOUND" ]]; then
            fail "ArgoCD app '${app}' not found"
        else
            fail "ArgoCD app '${app}' sync=${sync} health=${health}"
        fi
    done

    # No <org> placeholder
    if kubectl get applications -n argocd -o json 2>/dev/null | grep -q "<org>"; then
        fail "Found '<org>' placeholder in ArgoCD Application manifests"
    else
        pass "No '<org>' placeholder in ArgoCD Application manifests"
    fi

fi


# ── PHASE 3: Vault ────────────────────────────────────────────────────────────
header "Phase 3: Vault"

if require_cmd kubectl "Vault" && kubectl get pod vault-0 -n vault &>/dev/null; then

    vault_status=$(kubectl exec vault-0 -n vault -- vault status -format=json 2>/dev/null || echo '{"initialized":false}')

    initialized=$(echo "${vault_status}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('initialized','false'))" 2>/dev/null)
    sealed=$(echo "${vault_status}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('sealed','true'))" 2>/dev/null)

    [[ "$initialized" == "True" ]] && pass "Vault initialized" || fail "Vault not initialized"
    [[ "$sealed" == "False" ]] && pass "Vault unsealed" || fail "Vault sealed"

    # Check vault-agent sidecar in ml-agent pod
    if kubectl get pod -n ml-agent -l app=ml-agent -o jsonpath='{.items[0].spec.containers[*].name}' 2>/dev/null | grep -q "vault-agent"; then
        pass "Vault Agent sidecar present in ml-agent pods"
    else
        fail "Vault Agent sidecar NOT found in ml-agent pods"
    fi

    # No secrets in K8s Secrets
    if kubectl get secrets -A -o json 2>/dev/null | python3 -c "
import json,sys
data = json.load(sys.stdin)
cred_keys = {'password','db_dsn','dsn','webhook_url','slack_webhook','bootstrap_servers'}
found = []
for s in data['items']:
    name = s['metadata']['name']
    if any(skip in name for skip in ('default-token','kube-','sh.helm.')):
        continue
    for k in s.get('data', {}):
        if k.lower() in cred_keys:
            found.append(f\"{s['metadata']['namespace']}/{name}:{k}\")
if found:
    print('VIOLATIONS:' + ', '.join(found))
    sys.exit(1)
" 2>/dev/null; then
        pass "No credential K8s Secrets found"
    else
        fail "Credential K8s Secrets found — secrets must be in Vault only"
    fi

fi


# ── PHASE 5: Kafka ────────────────────────────────────────────────────────────
header "Phase 5: Kafka"

if require_cmd kubectl "Kafka"; then

    kafka_ready=$(kubectl get kafka anomaly-kafka -n kafka \
                  -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    [[ "$kafka_ready" == "True" ]] && \
        pass "Strimzi Kafka cluster Ready" || \
        fail "Strimzi Kafka cluster not Ready (status: ${kafka_ready:-NOT_FOUND})"

    for topic in "anomalies.flagged" "events.raw"; do
        topic_cr=$(echo "$topic" | tr '.' '-')
        status=$(kubectl get kafkatopic "${topic_cr}" -n kafka \
                 -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
        [[ "$status" == "True" ]] && \
            pass "KafkaTopic '${topic}' Ready" || \
            fail "KafkaTopic '${topic}' not Ready (status: ${status:-NOT_FOUND})"
    done

fi


# ── PHASE 6: KServe ───────────────────────────────────────────────────────────
header "Phase 6: KServe"

if require_cmd kubectl "KServe"; then

    is_ready=$(kubectl get inferenceservice anomaly-detector -n kserve \
               -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
    [[ "$is_ready" == "True" ]] && \
        pass "KServe InferenceService 'anomaly-detector' Ready" || \
        fail "KServe InferenceService not Ready (status: ${is_ready:-NOT_FOUND})"

    storage_uri=$(kubectl get inferenceservice anomaly-detector -n kserve \
                  -o jsonpath='{.spec.predictor.sklearn.storageUri}' 2>/dev/null || echo "")
    if echo "${storage_uri}" | grep -q "model-registry"; then
        pass "KServe storageUri points to model-registry: ${storage_uri}"
    else
        fail "KServe storageUri unexpected: ${storage_uri}"
    fi

fi


# ── PHASE 7: Agent ────────────────────────────────────────────────────────────
header "Phase 7: LangGraph Agent"

if require_cmd kubectl "Agent"; then

    ready=$(kubectl get deployment ml-agent -n ml-agent \
            -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
    desired=$(kubectl get deployment ml-agent -n ml-agent \
              -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "0")
    [[ "$ready" == "$desired" && "$ready" -gt "0" ]] && \
        pass "ml-agent deployment: ${ready}/${desired} replicas ready" || \
        fail "ml-agent deployment: ${ready}/${desired} ready replicas"

    # Check no vault 403 in recent logs
    pod=$(kubectl get pod -n ml-agent -l app=ml-agent \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [[ -n "$pod" ]]; then
        logs=$(kubectl logs "${pod}" -n ml-agent --tail=50 2>/dev/null || echo "")
        if echo "${logs}" | grep -q "403"; then
            fail "Vault 403 errors found in ml-agent logs — token renewal issue"
        else
            pass "No Vault 403 errors in ml-agent recent logs"
        fi
    else
        skip "No ml-agent pod found to check logs"
    fi

fi


# ── PHASE 8: Training CronJob ─────────────────────────────────────────────────
header "Phase 8: Training Pipeline"

if require_cmd kubectl "CronJob"; then

    cron_active=$(kubectl get cronjob monthly-training-trigger -n training-pipeline \
                  -o jsonpath='{.spec.suspend}' 2>/dev/null || echo "NOT_FOUND")
    if [[ "$cron_active" == "NOT_FOUND" ]]; then
        fail "Training CronJob 'monthly-training-trigger' not found"
    elif [[ "$cron_active" == "true" ]]; then
        fail "Training CronJob is suspended"
    else
        pass "Training CronJob 'monthly-training-trigger' active"
    fi

    cron_schedule=$(kubectl get cronjob monthly-training-trigger -n training-pipeline \
                    -o jsonpath='{.spec.schedule}' 2>/dev/null || echo "")
    [[ "$cron_schedule" == "0 2 1 * *" ]] && \
        pass "Training CronJob schedule: 0 2 1 * * (02:00 UTC, 1st of month)" || \
        fail "Training CronJob schedule unexpected: ${cron_schedule}"

fi


# ── PHASE 9: GitHub Actions ───────────────────────────────────────────────────
header "Phase 9: GitHub Actions Workflows"

for workflow in tf-pr.yml tf-apply.yml docker-agent.yml docker-training.yml; do
    wf_path=".github/workflows/${workflow}"
    if [[ -f "${wf_path}" ]]; then
        pass "Workflow '${workflow}' exists"

        # Check no <account-id> placeholder
        if grep -q "<account-id>" "${wf_path}"; then
            fail "Workflow '${workflow}' contains '<account-id>' placeholder"
        else
            pass "No '<account-id>' placeholder in '${workflow}'"
        fi
    else
        fail "Workflow '${workflow}' missing"
    fi
done


# ── PHASE 10: Observability ───────────────────────────────────────────────────
header "Phase 10: Monitoring"

if require_cmd kubectl "Monitoring"; then

    prom_ready=$(kubectl get statefulset -n monitoring -l app=prometheus \
                 -o jsonpath='{.items[0].status.readyReplicas}' 2>/dev/null || echo "0")
    [[ "$prom_ready" -gt "0" ]] && \
        pass "Prometheus StatefulSet has ${prom_ready} ready replica(s)" || \
        fail "Prometheus not ready (${prom_ready} ready replicas)"

    grafana_ready=$(kubectl get deployment -n monitoring -l app.kubernetes.io/name=grafana \
                    -o jsonpath='{.items[0].status.readyReplicas}' 2>/dev/null || echo "0")
    [[ "$grafana_ready" -gt "0" ]] && \
        pass "Grafana deployment ready" || \
        fail "Grafana not ready"

fi


# ── Security spot checks ──────────────────────────────────────────────────────
header "Security Spot Checks"

# vault-init.json not in git
if git log --all -- vault-init.json 2>/dev/null | grep -q "commit"; then
    fail "vault-init.json found in git history — SECURITY RISK"
else
    pass "vault-init.json not in git history"
fi

# No real AWS keys in source
if grep -r "AKIA[0-9A-Z]\{16\}" --include="*.py" --include="*.tf" --include="*.yaml" . 2>/dev/null | grep -v ".git" | grep -q .; then
    fail "AWS access key ID (AKIA...) found in source files"
else
    pass "No AWS access key IDs in source files"
fi

# No hardcoded Slack webhooks
if grep -r "hooks\.slack\.com/services/[A-Z0-9]\{8,\}" --include="*.py" --include="*.yaml" . 2>/dev/null | grep -v ".git" | grep -v "XXX" | grep -q .; then
    fail "Real Slack webhook URL found in source"
else
    pass "No real Slack webhook URLs in source"
fi


# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo -e "${GREEN}PASSED:${NC} ${PASS}  ${RED}FAILED:${NC} ${FAIL}  ${YELLOW}SKIPPED:${NC} ${SKIP}"
echo "========================================"

if [[ $FAIL -gt 0 ]]; then
    echo -e "${RED}RESULT: FAIL${NC} — ${FAIL} check(s) failed"
    exit 1
else
    echo -e "${GREEN}RESULT: PASS${NC} — all checks passed"
    exit 0
fi
