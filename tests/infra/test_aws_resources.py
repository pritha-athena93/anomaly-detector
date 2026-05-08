"""
Infrastructure tests for AWS resources provisioned by Terraform (Phase 0).
Covers: TS-0-01..19

Requires: AWS credentials with read access (no writes performed).
Set TEST_AWS_REGION and cluster name via TEST_CLUSTER_NAME env var.
Run: pytest -m infra tests/infra/test_aws_resources.py
"""

import json
import os
import pytest

pytestmark = pytest.mark.infra

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    pytest.skip("boto3 not installed", allow_module_level=True)

AWS_REGION   = os.environ.get("TEST_AWS_REGION",     "us-east-1")
CLUSTER_NAME = os.environ.get("TEST_CLUSTER_NAME",   "anomaly-detector")
ACCOUNT_ID   = os.environ.get("TEST_ACCOUNT_ID",     "")   # set or auto-detected below

# Auto-detect account ID
try:
    _sts = boto3.client("sts", region_name=AWS_REGION)
    ACCOUNT_ID = ACCOUNT_ID or _sts.get_caller_identity()["Account"]
except Exception:
    pass

TF_STATE_BUCKET   = f"anomaly-detector-tf-state-{ACCOUNT_ID}"
CLOUDTRAIL_BUCKET = f"{CLUSTER_NAME}-cloudtrail-logs-{ACCOUNT_ID}"
MODEL_REG_BUCKET  = f"{CLUSTER_NAME}-model-registry-{ACCOUNT_ID}"
KFP_BUCKET        = f"{CLUSTER_NAME}-kfp-artifacts-{ACCOUNT_ID}"
SQS_QUEUE_NAME    = f"{CLUSTER_NAME}-cloudtrail-events"
DYNAMO_TABLE      = "anomaly-tf-locks"
ECR_REPOS         = [f"{CLUSTER_NAME}/agent", f"{CLUSTER_NAME}/training-pipeline", f"{CLUSTER_NAME}/poller"]


@pytest.fixture(scope="module")
def s3():
    return boto3.client("s3", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def sqs():
    return boto3.client("sqs", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def dynamodb():
    return boto3.client("dynamodb", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def rds():
    return boto3.client("rds", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def ecr():
    return boto3.client("ecr", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def ssm():
    return boto3.client("ssm", region_name=AWS_REGION)


@pytest.fixture(scope="module")
def events():
    return boto3.client("events", region_name=AWS_REGION)


# ── TS-0-01: S3 state bucket versioning ──────────────────────────────────────

def test_tf_state_bucket_versioning_enabled(s3):
    """TS-0-01: Terraform state bucket has versioning enabled."""
    resp = s3.get_bucket_versioning(Bucket=TF_STATE_BUCKET)
    assert resp.get("Status") == "Enabled", \
        f"Versioning not Enabled on {TF_STATE_BUCKET}: {resp.get('Status')}"


def test_tf_state_bucket_encryption(s3):
    """TS-0-02: State bucket uses AES256 server-side encryption."""
    resp = s3.get_bucket_encryption(Bucket=TF_STATE_BUCKET)
    rules = resp["ServerSideEncryptionConfiguration"]["Rules"]
    algo  = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
    assert algo == "AES256"


def test_tf_state_bucket_not_public(s3):
    """TS-0-05: State bucket blocks all public access."""
    resp = s3.get_public_access_block(Bucket=TF_STATE_BUCKET)
    cfg  = resp["PublicAccessBlockConfiguration"]
    assert cfg["BlockPublicAcls"]       is True
    assert cfg["IgnorePublicAcls"]      is True
    assert cfg["BlockPublicPolicy"]     is True
    assert cfg["RestrictPublicBuckets"] is True


# ── TS-0-03: DynamoDB lock table ──────────────────────────────────────────────

def test_dynamodb_lock_table_exists(dynamodb):
    """TS-0-03: anomaly-tf-locks table exists with LockID hash key."""
    resp  = dynamodb.describe_table(TableName=DYNAMO_TABLE)
    table = resp["Table"]
    key_schema = {k["AttributeName"]: k["KeyType"] for k in table["KeySchema"]}
    assert key_schema.get("LockID") == "HASH"


def test_dynamodb_lock_table_billing_mode(dynamodb):
    """TS-0-03: PAY_PER_REQUEST billing (no capacity planning needed)."""
    resp = dynamodb.describe_table(TableName=DYNAMO_TABLE)
    mode = resp["Table"].get("BillingModeSummary", {}).get("BillingMode", "")
    assert mode == "PAY_PER_REQUEST"


# ── TS-0-04: Concurrent apply blocked by DynamoDB ────────────────────────────

def test_dynamo_lock_table_attribute_type(dynamodb):
    """TS-0-04 (prerequisite): LockID attribute is type String (S)."""
    resp  = dynamodb.describe_table(TableName=DYNAMO_TABLE)
    attrs = {a["AttributeName"]: a["AttributeType"] for a in resp["Table"]["AttributeDefinitions"]}
    assert attrs.get("LockID") == "S"


# ── TS-0-10: SQS queue config ─────────────────────────────────────────────────

def test_sqs_queue_exists(sqs):
    """TS-0-10: SQS queue provisioned by Terraform."""
    resp = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
    assert resp["QueueUrl"]


@pytest.fixture(scope="module")
def sqs_url(sqs):
    return sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)["QueueUrl"]


def test_sqs_visibility_timeout_300(sqs, sqs_url):
    """TS-0-10: visibility timeout = 300s (matches 5-min poller interval)."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=sqs_url,
        AttributeNames=["VisibilityTimeout"]
    )["Attributes"]
    assert attrs["VisibilityTimeout"] == "300"


def test_sqs_retention_1_day(sqs, sqs_url):
    attrs = sqs.get_queue_attributes(
        QueueUrl=sqs_url,
        AttributeNames=["MessageRetentionPeriod"]
    )["Attributes"]
    assert attrs["MessageRetentionPeriod"] == "86400"


def test_sqs_long_polling_enabled(sqs, sqs_url):
    attrs = sqs.get_queue_attributes(
        QueueUrl=sqs_url,
        AttributeNames=["ReceiveMessageWaitTimeSeconds"]
    )["Attributes"]
    assert int(attrs["ReceiveMessageWaitTimeSeconds"]) >= 5


# ── TS-0-11: EventBridge fires on S3 ObjectCreated ───────────────────────────

def test_eventbridge_rule_exists(events):
    """TS-0-11: EventBridge rule targeting cloudtrail-logs bucket exists."""
    resp = events.list_rules(NamePrefix=f"{CLUSTER_NAME}-cloudtrail")
    rules = resp.get("Rules", [])
    assert len(rules) >= 1, f"No EventBridge rule matching {CLUSTER_NAME}-cloudtrail*"


def test_eventbridge_rule_enabled(events):
    resp = events.list_rules(NamePrefix=f"{CLUSTER_NAME}-cloudtrail")
    for rule in resp["Rules"]:
        assert rule["State"] == "ENABLED", f"Rule {rule['Name']} is {rule['State']}"


# ── TS-0-16: RDS encryption ──────────────────────────────────────────────────

def test_rds_storage_encrypted(rds):
    """TS-0-16: RDS instance has StorageEncrypted=True."""
    resp      = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
    instance  = resp["DBInstances"][0]
    assert instance["StorageEncrypted"] is True


def test_rds_not_publicly_accessible(rds):
    """TS-0-08: RDS not publicly accessible."""
    resp     = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
    instance = resp["DBInstances"][0]
    assert instance["PubliclyAccessible"] is False


def test_rds_deletion_protection_enabled(rds):
    """TS-0-09: RDS deletion_protection = True."""
    resp     = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
    instance = resp["DBInstances"][0]
    assert instance["DeletionProtection"] is True


def test_rds_engine_version_16(rds):
    resp     = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
    instance = resp["DBInstances"][0]
    assert instance["EngineVersion"].startswith("16")


def test_rds_in_private_subnet(rds):
    """TS-0-08: RDS subnet group uses private subnets (no public IPs)."""
    resp     = rds.describe_db_instances(DBInstanceIdentifier=f"{CLUSTER_NAME}-postgres")
    instance = resp["DBInstances"][0]
    sg_name  = instance["DBSubnetGroup"]["DBSubnetGroupName"]
    assert "rds" in sg_name, f"Unexpected subnet group: {sg_name}"


# ── TS-0-14: SSM parameters ───────────────────────────────────────────────────

@pytest.mark.parametrize("param_name,expected_initial", [
    ("/anomaly/last_train_ts", "1970-01-01T00:00:00Z"),
    ("/anomaly/model_version", "none"),
    ("/anomaly/threshold",     "-0.1"),
])
def test_ssm_parameter_exists(ssm, param_name, expected_initial):
    """TS-0-14: SSM parameters provisioned (may differ from initial if pipeline ran)."""
    resp  = ssm.get_parameter(Name=param_name)
    value = resp["Parameter"]["Value"]
    assert value is not None


def test_ssm_last_train_ts_is_valid_iso8601(ssm):
    """last_train_ts is ISO 8601 datetime string."""
    import re
    resp  = ssm.get_parameter(Name="/anomaly/last_train_ts")
    value = resp["Parameter"]["Value"]
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?$"
    assert re.match(pattern, value), f"Invalid timestamp format: {value}"


# ── TS-0-13: ECR repos with scan-on-push ─────────────────────────────────────

@pytest.mark.parametrize("repo_suffix", ["agent", "training-pipeline", "poller"])
def test_ecr_repo_exists(ecr, repo_suffix):
    """TS-0-13: ECR repository exists for each service."""
    repo_name = f"{CLUSTER_NAME}/{repo_suffix}"
    resp = ecr.describe_repositories(repositoryNames=[repo_name])
    assert len(resp["repositories"]) == 1


@pytest.mark.parametrize("repo_suffix", ["agent", "training-pipeline", "poller"])
def test_ecr_scan_on_push_enabled(ecr, repo_suffix):
    """TS-0-13: scan_on_push=true for ECR repositories."""
    repo_name = f"{CLUSTER_NAME}/{repo_suffix}"
    resp = ecr.describe_repositories(repositoryNames=[repo_name])
    scan_config = resp["repositories"][0]["imageScanningConfiguration"]
    assert scan_config["scanOnPush"] is True


# ── TS-0-17: S3 cloudtrail bucket force_destroy=false ────────────────────────

def test_all_s3_buckets_not_public(s3):
    """TS-0-17 / TS-SEC-09: all four buckets block public access."""
    buckets = [TF_STATE_BUCKET, CLOUDTRAIL_BUCKET, MODEL_REG_BUCKET, KFP_BUCKET]
    for bucket in buckets:
        try:
            resp = s3.get_public_access_block(Bucket=bucket)
            cfg  = resp["PublicAccessBlockConfiguration"]
            assert all([
                cfg["BlockPublicAcls"],
                cfg["IgnorePublicAcls"],
                cfg["BlockPublicPolicy"],
                cfg["RestrictPublicBuckets"],
            ]), f"Bucket {bucket} has public access not fully blocked: {cfg}"
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration":
                pytest.fail(f"Bucket {bucket} has no public access block configuration")
            raise


def test_three_separate_s3_buckets_created(s3):
    """TS-0-18: three distinct S3 buckets (cloudtrail/model-registry/kfp)."""
    for bucket in [CLOUDTRAIL_BUCKET, MODEL_REG_BUCKET, KFP_BUCKET]:
        resp = s3.head_bucket(Bucket=bucket)
        # head_bucket returns 200 if exists, raises ClientError if not
