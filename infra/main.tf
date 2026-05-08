terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # S3 backend for remote state (required for GitHub Actions CI/CD).
  # Bootstrap ONCE before first `terraform init`:
  #   bash scripts/bootstrap-tf-state.sh <account-id> us-east-1
  #
  # To migrate from local state:
  #   terraform init -migrate-state
  backend "s3" {
    bucket = "anomaly-detector-tf-state-984445750473"   # substitute account ID
    key    = "anomaly-detector/terraform.tfstate"
    region = "us-east-1"
    encrypt = true
    use_lockfile = true   # native S3 locking (Terraform >= 1.10, no DynamoDB needed)
  }
}

# NOTE: helm and kubernetes providers removed.
# EKS endpoint_public_access = false → Terraform cannot reach the K8s API from outside
# the VPC. All K8s/Helm resources are managed by ArgoCD (bootstrapped from the bastion).
# See scripts/bootstrap-aws.sh.

provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

# ── VPC ──────────────────────────────────────────────────
# Private cluster: nodes only in private subnets.
# Public subnets exist for the NAT GW + bastion only.

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = { Name = var.cluster_name }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = var.cluster_name }
}

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  map_public_ip_on_launch = true

  tags = {
    Name                                        = "${var.cluster_name}-public-${count.index}"
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.${count.index + 10}.0/24"
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name                                        = "${var.cluster_name}-private-${count.index}"
    "kubernetes.io/role/internal-elb"           = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_eip" "nat" {
  domain     = "vpc"
  depends_on = [aws_internet_gateway.main]
  tags       = { Name = "${var.cluster_name}-nat" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = { Name = var.cluster_name }
  depends_on    = [aws_internet_gateway.main]
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "${var.cluster_name}-public" }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "${var.cluster_name}-private" }
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# ── S3: Three purpose-scoped buckets ─────────────────────
# cloudtrail-logs — CloudTrail delivers raw log files here; EventBridge notifies SQS
# model-registry  — versioned model artifacts (KServe reads, KFP writes)
# kfp-artifacts   — Kubeflow Pipeline run artifacts + Katib trial data

locals {
  buckets = {
    cloudtrail_logs = "${var.cluster_name}-cloudtrail-${data.aws_caller_identity.current.account_id}"
    model_registry  = "${var.cluster_name}-models-${data.aws_caller_identity.current.account_id}"
    kfp_artifacts   = "${var.cluster_name}-kfp-${data.aws_caller_identity.current.account_id}"
  }
}

resource "aws_s3_bucket" "cloudtrail_logs" {
  bucket        = local.buckets.cloudtrail_logs
  force_destroy = false
  tags          = { Name = "${var.cluster_name}-cloudtrail-logs" }
}

resource "aws_s3_bucket" "model_registry" {
  bucket        = local.buckets.model_registry
  force_destroy = false
  tags          = { Name = "${var.cluster_name}-model-registry" }
}

resource "aws_s3_bucket" "kfp_artifacts" {
  bucket        = local.buckets.kfp_artifacts
  force_destroy = false
  tags          = { Name = "${var.cluster_name}-kfp-artifacts" }
}

resource "aws_s3_bucket_versioning" "model_registry" {
  bucket = aws_s3_bucket.model_registry.id
  versioning_configuration { status = "Enabled" }
}

# Encryption on all three
resource "aws_s3_bucket_server_side_encryption_configuration" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "model_registry" {
  bucket = aws_s3_bucket.model_registry.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "kfp_artifacts" {
  bucket = aws_s3_bucket.kfp_artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block public access on all three
resource "aws_s3_bucket_public_access_block" "cloudtrail_logs" {
  bucket                  = aws_s3_bucket.cloudtrail_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "model_registry" {
  bucket                  = aws_s3_bucket.model_registry.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "kfp_artifacts" {
  bucket                  = aws_s3_bucket.kfp_artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CloudTrail bucket policy: allow CloudTrail service to write
resource "aws_s3_bucket_policy" "cloudtrail_logs" {
  bucket = aws_s3_bucket.cloudtrail_logs.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AWSCloudTrailAclCheck"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:GetBucketAcl"
        Resource  = aws_s3_bucket.cloudtrail_logs.arn
      },
      {
        Sid       = "AWSCloudTrailWrite"
        Effect    = "Allow"
        Principal = { Service = "cloudtrail.amazonaws.com" }
        Action    = "s3:PutObject"
        Resource  = "${aws_s3_bucket.cloudtrail_logs.arn}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
        Condition = {
          StringEquals = { "s3:x-amz-acl" = "bucket-owner-full-control" }
        }
      }
    ]
  })
}

# ── SQS: CloudTrail events queue + DLQ ───────────────────
# Gap-5 fix (no SQS DLQ): malformed S3 events no longer retry indefinitely.
# After maxReceiveCount=3 delivery attempts, message moves to DLQ.
# Ops team monitors DLQ depth via CloudWatch; alert on non-zero depth.
#
# DLQ must exist before main queue (Terraform handles ordering via depends_on).

resource "aws_sqs_queue" "cloudtrail_events_dlq" {
  name                      = "${var.cluster_name}-cloudtrail-events-dlq"
  message_retention_seconds = 1209600  # 14 days — enough time for ops investigation
  tags                      = { Name = "${var.cluster_name}-cloudtrail-events-dlq" }
}

resource "aws_sqs_queue" "cloudtrail_events" {
  name                       = "${var.cluster_name}-cloudtrail-events"
  visibility_timeout_seconds = 300   # 5 min — matches poller run interval
  message_retention_seconds  = 86400 # 1 day

  # Gap-5 fix: after 3 failed receive attempts, move to DLQ
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.cloudtrail_events_dlq.arn
    maxReceiveCount     = 3
  })

  tags = { Name = "${var.cluster_name}-cloudtrail-events" }
}

# ── RDS: Postgres 16 + pgvector ───────────────────────────
# Private subnet only; accessible from EKS nodes (same VPC) only.
# pgvector: IVFFlat index on embedding column (shared_preload_libraries).

resource "aws_security_group" "rds" {
  name   = "${var.cluster_name}-rds"
  vpc_id = aws_vpc.main.id

  ingress {
    description = "Postgres from VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [aws_vpc.main.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.cluster_name}-rds" }
}

resource "aws_db_subnet_group" "main" {
  name       = var.cluster_name
  subnet_ids = aws_subnet.private[*].id
  tags       = { Name = "${var.cluster_name}-rds" }
}

# Enable pgvector extension via parameter group
resource "aws_db_parameter_group" "pgvector" {
  name   = "${var.cluster_name}-pgvector"
  family = "postgres16"

  parameter {
    name         = "shared_preload_libraries"
    value        = "pg_stat_statements"
    apply_method = "pending-reboot"
  }
}

resource "random_password" "rds" {
  length  = 32
  special = false # Postgres passwords with special chars need URL-encoding — skip
}

resource "aws_db_instance" "postgres" {
  identifier             = "${var.cluster_name}-postgres"
  engine                 = "postgres"
  engine_version         = "16.3"
  instance_class         = "db.t3.medium"
  allocated_storage      = 50
  max_allocated_storage  = 200   # autoscaling up to 200GB
  storage_type           = "gp3"
  storage_encrypted      = true

  db_name  = "anomaly_db"
  username = "anomaly_user"
  password = random_password.rds.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  parameter_group_name   = aws_db_parameter_group.pgvector.name

  multi_az              = false  # upgrade to true for production HA
  publicly_accessible   = false
  skip_final_snapshot   = true   # set to false before prod go-live
  deletion_protection   = false  # set to true before prod go-live

  tags = { Name = "${var.cluster_name}-postgres" }
}

# ── Secrets Manager ───────────────────────────────────────
# RDS password auto-generated, stored as JSON DSN for easy agent consumption.
# PagerDuty and Slack secrets created empty — populate manually after apply.

resource "aws_secretsmanager_secret" "rds_password" {
  name                    = "anomaly/rds-password"
  recovery_window_in_days = 0   # immediate delete (dev); set to 30 in prod
}

resource "aws_secretsmanager_secret_version" "rds_password" {
  secret_id = aws_secretsmanager_secret.rds_password.id
  # Full DSN so consumers can use it directly without string concatenation
  secret_string = jsonencode({
    dsn      = "postgresql://anomaly_user:${random_password.rds.result}@${aws_db_instance.postgres.address}:5432/anomaly_db"
    username = "anomaly_user"
    password = random_password.rds.result
    host     = aws_db_instance.postgres.address
    port     = 5432
    dbname   = "anomaly_db"
  })
}

# Created empty — populate via: aws secretsmanager put-secret-value --secret-id anomaly/pagerduty-key --secret-string '{"key":"pd_..."}'
resource "aws_secretsmanager_secret" "pagerduty_key" {
  name                    = "anomaly/pagerduty-key"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret" "slack_webhook" {
  name                    = "anomaly/slack-webhook"
  recovery_window_in_days = 0
}

# ── SSM Parameters ────────────────────────────────────────
# Seeded to epoch zero; training pipeline overwrites last_train_ts after each run.
# Agent reads last_train_ts to scope CloudTrail RAG window.

resource "aws_ssm_parameter" "last_train_ts" {
  name  = "/anomaly/last_train_ts"
  type  = "String"
  value = "1970-01-01T00:00:00Z"
}

resource "aws_ssm_parameter" "model_version" {
  name  = "/anomaly/model_version"
  type  = "String"
  value = "none"
}

resource "aws_ssm_parameter" "threshold" {
  name  = "/anomaly/threshold"
  type  = "String"
  value = "-0.1"  # IsolationForest decision_function threshold
}

# ── IAM: EKS Cluster + Node Roles ────────────────────────

resource "aws_iam_role" "eks_cluster" {
  name = "${var.cluster_name}-eks-cluster"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster" {
  role       = aws_iam_role.eks_cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role" "eks_nodes" {
  name = "${var.cluster_name}-eks-nodes"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_worker_node" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "eks_cni" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "eks_ecr" {
  role       = aws_iam_role.eks_nodes.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

# ── EKS Cluster ───────────────────────────────────────────
# Private cluster: endpoint_public_access = false.
# All kubectl/helm commands must run from within the VPC (bastion or CI runner in VPC).

resource "aws_eks_cluster" "primary" {
  name     = var.cluster_name
  version  = "1.31"
  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids              = concat(aws_subnet.private[*].id, aws_subnet.public[*].id)
    endpoint_private_access = true
    endpoint_public_access  = false  # private cluster — access via bastion only
  }

  # API_AND_CONFIG_MAP: allows both aws-auth ConfigMap and EKS access entry API.
  # Required for aws_eks_access_entry below (bastion cluster-admin grant).
  access_config {
    authentication_mode = "API_AND_CONFIG_MAP"
  }

  # access_config can only be set once on cluster creation — updates to
  # authentication_mode are done in-place by the AWS API, but Terraform
  # < 5.x may show a force-replace diff if the attribute wasn't in the
  # original state. Ignore it to prevent unintended cluster recreation.
  lifecycle {
    ignore_changes = [access_config]
  }

  depends_on = [aws_iam_role_policy_attachment.eks_cluster]
}

# Grant bastion role cluster-admin so bootstrap-aws.sh can run kubectl/helm.
# Using EKS access entry API (no manual aws-auth ConfigMap editing needed).
resource "aws_eks_access_entry" "bastion" {
  cluster_name  = aws_eks_cluster.primary.name
  principal_arn = aws_iam_role.bastion.arn
  type          = "STANDARD"

  depends_on = [aws_eks_cluster.primary]
}

resource "aws_eks_access_policy_association" "bastion_admin" {
  cluster_name  = aws_eks_cluster.primary.name
  principal_arn = aws_iam_role.bastion.arn
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.bastion]
}

# Allow bastion SG to reach EKS private API endpoint on 443.
# Without this rule the bastion kubectl calls time out.
resource "aws_security_group_rule" "eks_from_bastion" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_eks_cluster.primary.vpc_config[0].cluster_security_group_id
  source_security_group_id = aws_security_group.bastion.id
  description              = "Bastion to EKS API"
}

# ── EKS Addons ───────────────────────────────────────────
# EBS CSI driver: required for gp3 PersistentVolumes (Vault, Prometheus).
# IRSA: dedicated role for ebs-csi-controller-sa (NOT the node role).

resource "aws_iam_role" "ebs_csi" {
  name = "${var.cluster_name}-ebs-csi"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Condition = {
        StringEquals = {
          "${local.oidc_issuer}:sub" = "system:serviceaccount:kube-system:ebs-csi-controller-sa"
          "${local.oidc_issuer}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}

resource "aws_eks_addon" "ebs_csi" {
  cluster_name             = aws_eks_cluster.primary.name
  addon_name               = "aws-ebs-csi-driver"
  service_account_role_arn = aws_iam_role.ebs_csi.arn
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [
    aws_eks_node_group.ml,
    aws_iam_role_policy_attachment.ebs_csi,
  ]
}

# ── OIDC Provider (IRSA) ──────────────────────────────────

data "tls_certificate" "eks" {
  url = aws_eks_cluster.primary.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "eks" {
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.eks.certificates[0].sha1_fingerprint]
  url             = aws_eks_cluster.primary.identity[0].oidc[0].issuer
}

# ── IRSA: Per-ServiceAccount IAM Roles ───────────────────
# Least-privilege: each SA gets only the permissions it needs.
# Annotate each K8s SA with: eks.amazonaws.com/role-arn: <role_arn>

locals {
  oidc_issuer = replace(aws_eks_cluster.primary.identity[0].oidc[0].issuer, "https://", "")
}

# Helper: build IRSA trust policy for a single namespace:serviceaccount
locals {
  irsa_trust = { for k, v in {
    kserve_sa      = { ns = "kserve",          sa = "kserve-sa"       }
    poller_sa      = { ns = "anomaly-poller",   sa = "poller-sa"       }
    agent_sa       = { ns = "ml-agent",         sa = "agent-sa"        }
    kfp_pipeline   = { ns = "kubeflow",         sa = "kfp-pipeline-sa" }
  } : k => jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Condition = {
        StringEquals = {
          "${local.oidc_issuer}:sub" = "system:serviceaccount:${v.ns}:${v.sa}"
          "${local.oidc_issuer}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })}
}

# kserve-sa: read model artifacts from model-registry bucket only
resource "aws_iam_role" "kserve_sa" {
  name               = "${var.cluster_name}-kserve-sa"
  assume_role_policy = local.irsa_trust["kserve_sa"]
}

resource "aws_iam_role_policy" "kserve_sa" {
  name = "kserve-model-read"
  role = aws_iam_role.kserve_sa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.model_registry.arn, "${aws_s3_bucket.model_registry.arn}/*"]
    }]
  })
}

# poller-sa: drain SQS + read raw CloudTrail logs from S3
resource "aws_iam_role" "poller_sa" {
  name               = "${var.cluster_name}-poller-sa"
  assume_role_policy = local.irsa_trust["poller_sa"]
}

resource "aws_iam_role_policy" "poller_sa" {
  name = "poller-sqs-s3"
  role = aws_iam_role.poller_sa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = aws_sqs_queue.cloudtrail_events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.cloudtrail_logs.arn, "${aws_s3_bucket.cloudtrail_logs.arn}/*"]
      },
    ]
  })
}

# agent-sa: Bedrock invoke + SSM read + Secrets Manager read
resource "aws_iam_role" "agent_sa" {
  name               = "${var.cluster_name}-agent-sa"
  assume_role_policy = local.irsa_trust["agent_sa"]
}

resource "aws_iam_role_policy" "agent_sa" {
  name = "agent-bedrock-ssm-secrets"
  role = aws_iam_role.agent_sa.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
        Resource = "arn:aws:bedrock:${var.region}::foundation-model/*"
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:GetParameters"]
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/anomaly/*"
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [
          aws_secretsmanager_secret.rds_password.arn,
          aws_secretsmanager_secret.pagerduty_key.arn,
          aws_secretsmanager_secret.slack_webhook.arn,
        ]
      },
    ]
  })
}

# kfp-pipeline-sa: full S3 on kfp + model-registry, SSM write, KServe patch
resource "aws_iam_role" "kfp_pipeline" {
  name               = "${var.cluster_name}-kfp-pipeline-sa"
  assume_role_policy = local.irsa_trust["kfp_pipeline"]
}

resource "aws_iam_role_policy" "kfp_pipeline" {
  name = "kfp-pipeline-s3-ssm"
  role = aws_iam_role.kfp_pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.kfp_artifacts.arn,   "${aws_s3_bucket.kfp_artifacts.arn}/*",
          aws_s3_bucket.model_registry.arn,  "${aws_s3_bucket.model_registry.arn}/*",
          aws_s3_bucket.cloudtrail_logs.arn, "${aws_s3_bucket.cloudtrail_logs.arn}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:PutParameter", "ssm:GetParameter"]
        Resource = "arn:aws:ssm:${var.region}:${data.aws_caller_identity.current.account_id}:parameter/anomaly/*"
      },
      {
        Effect   = "Allow"
        Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
        Resource = "arn:aws:bedrock:${var.region}::foundation-model/*"
      },
      {
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster"]
        Resource = aws_eks_cluster.primary.arn
      },
      {
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = aws_secretsmanager_secret.rds_password.arn
      },
    ]
  })
}

# ── Node Groups ───────────────────────────────────────────

resource "aws_eks_node_group" "ml" {
  cluster_name    = aws_eks_cluster.primary.name
  node_group_name = "ml-pool"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.private[*].id

  instance_types = ["m5.xlarge"]
  disk_size      = 50

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 3
  }

  labels = { node-role = "ml" }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_ecr,
  ]
}

resource "aws_eks_node_group" "kserve" {
  cluster_name    = aws_eks_cluster.primary.name
  node_group_name = "kserve-pool"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.private[*].id

  instance_types = ["m5.xlarge"]
  disk_size      = 50

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 2
  }

  labels = { node-role = "kserve" }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_ecr,
  ]
}

resource "aws_eks_node_group" "monitoring" {
  cluster_name    = aws_eks_cluster.primary.name
  node_group_name = "monitoring-pool"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = aws_subnet.private[*].id

  instance_types = ["t3.medium"]
  disk_size      = 30

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 1
  }

  labels = { node-role = "monitoring" }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_ecr,
  ]
}

# ── ECR Repositories ──────────────────────────────────────
# One repo per image. Scan on push for CVEs.
# Images tagged :latest + :<git-sha> by GitHub Actions.

resource "aws_ecr_repository" "agent" {
  name                 = "${var.cluster_name}/agent"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  tags = { Name = "${var.cluster_name}-agent" }
}

resource "aws_ecr_repository" "poller" {
  name                 = "${var.cluster_name}/poller"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  tags = { Name = "${var.cluster_name}-poller" }
}

resource "aws_ecr_repository" "training_pipeline" {
  name                 = "${var.cluster_name}/training-pipeline"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
  tags = { Name = "${var.cluster_name}-training-pipeline" }
}

# ── EventBridge → SQS (CloudTrail S3 delivery notification) ──
# S3 Event Notifications → EventBridge → SQS → poller CronJob.
# Requires: S3 bucket EventBridge notifications enabled (done via aws_s3_bucket_notification).

resource "aws_cloudwatch_event_rule" "cloudtrail_s3" {
  name        = "${var.cluster_name}-cloudtrail-new-log"
  description = "Fires when CloudTrail delivers a new log file to S3"
  event_pattern = jsonencode({
    source      = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.cloudtrail_logs.bucket] }
      object = { key = [{ suffix = ".json.gz" }] }
    }
  })
  tags = { Name = "${var.cluster_name}-cloudtrail-new-log" }
}

resource "aws_cloudwatch_event_target" "cloudtrail_to_sqs" {
  rule      = aws_cloudwatch_event_rule.cloudtrail_s3.name
  target_id = "cloudtrail-to-sqs"
  arn       = aws_sqs_queue.cloudtrail_events.arn
}

# Enable EventBridge on the cloudtrail-logs S3 bucket
resource "aws_s3_bucket_notification" "cloudtrail_logs" {
  bucket      = aws_s3_bucket.cloudtrail_logs.id
  eventbridge = true
}

# Allow EventBridge to send to SQS
resource "aws_sqs_queue_policy" "cloudtrail_events_policy" {
  queue_url = aws_sqs_queue.cloudtrail_events.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowEventBridge"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.cloudtrail_events.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_cloudwatch_event_rule.cloudtrail_s3.arn }
      }
    }]
  })
}

# ── GitHub Actions IAM Roles (OIDC — no static keys) ──────
# Two roles: one for Terraform apply, one for ECR push.
# Trust: GitHub's OIDC provider (token.actions.githubusercontent.com).

resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

locals {
  # Replace pritha-athena93/anomaly-detector with your actual GitHub org/repo
  github_repo     = "pritha-athena93/anomaly-detector"
  github_oidc_arn = aws_iam_openid_connect_provider.github.arn
}

resource "aws_iam_role" "github_terraform" {
  name = "${var.cluster_name}-github-terraform"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.github_oidc_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${local.github_repo}:ref:refs/heads/main"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_terraform" {
  name = "terraform-apply"
  role = aws_iam_role.github_terraform.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["ec2:*", "eks:*", "iam:*", "s3:*", "sqs:*", "rds:*",
                  "secretsmanager:*", "ssm:*", "ecr:*", "events:*",
                  "sts:GetCallerIdentity", "dynamodb:*"]
      Resource = "*"
    }]
  })
}

resource "aws_iam_role" "github_ecr" {
  name = "${var.cluster_name}-github-ecr"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.github_oidc_arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${local.github_repo}:ref:refs/heads/main"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "github_ecr" {
  name = "ecr-push"
  role = aws_iam_role.github_ecr.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["ecr:GetAuthorizationToken"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ecr:BatchCheckLayerAvailability", "ecr:PutImage",
                    "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
                    "ecr:CompleteLayerUpload", "ecr:DescribeRepositories",
                    "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
        Resource = [
          aws_ecr_repository.agent.arn,
          aws_ecr_repository.poller.arn,
          aws_ecr_repository.training_pipeline.arn,
        ]
      }
    ]
  })
}

# ── Vault Auto-Unseal (AWS KMS) ───────────────────────────
# Vault uses this KMS key to encrypt/decrypt its master key on startup.
# Eliminates manual unseal — pods can restart freely without human intervention.

resource "aws_kms_key" "vault" {
  description             = "${var.cluster_name} Vault auto-unseal"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  tags = {
    Name    = "${var.cluster_name}-vault-unseal"
    Cluster = var.cluster_name
  }
}

resource "aws_kms_alias" "vault" {
  name          = "alias/${var.cluster_name}-vault-unseal"
  target_key_id = aws_kms_key.vault.key_id
}

# IRSA role for vault SA — allows KMS auto-unseal only
resource "aws_iam_role" "vault_sa" {
  name = "${var.cluster_name}-vault-sa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.eks.arn }
      Condition = {
        StringEquals = {
          "${local.oidc_issuer}:sub" = "system:serviceaccount:vault:vault"
          "${local.oidc_issuer}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })
}

resource "aws_iam_role_policy" "vault_kms" {
  name = "vault-kms-unseal"
  role = aws_iam_role.vault_sa.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:DescribeKey",
      ]
      Resource = aws_kms_key.vault.arn
    }]
  })
}

output "vault_kms_key_arn" {
  value       = aws_kms_key.vault.arn
  description = "KMS key ARN for Vault auto-unseal (put in vault Helm values)"
}

output "vault_irsa_role_arn" {
  value       = aws_iam_role.vault_sa.arn
  description = "IRSA role ARN for vault SA (annotate vault ServiceAccount)"
}
