output "cluster_name" {
  value = aws_eks_cluster.primary.name
}

output "kubeconfig_command" {
  description = "Run from inside VPC (bastion) — cluster is private"
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${aws_eks_cluster.primary.name}"
}

# ── S3 buckets ────────────────────────────────────────────

output "cloudtrail_logs_bucket" {
  value = aws_s3_bucket.cloudtrail_logs.bucket
}

output "model_registry_bucket" {
  value = aws_s3_bucket.model_registry.bucket
}

output "kfp_artifacts_bucket" {
  value = aws_s3_bucket.kfp_artifacts.bucket
}

output "kserve_storage_uri" {
  value = "s3://${aws_s3_bucket.model_registry.bucket}/anomaly-detector/latest"
}

# ── RDS ───────────────────────────────────────────────────

output "rds_endpoint" {
  value     = aws_db_instance.postgres.address
  sensitive = true
}

output "rds_dsn_secret_arn" {
  description = "Secrets Manager ARN for the full DSN JSON — use in K8s ExternalSecret or pod env"
  value       = aws_secretsmanager_secret.rds_password.arn
}

# ── SQS ───────────────────────────────────────────────────

output "sqs_queue_url" {
  description = "Set as SQS_URL env var in the poller CronJob"
  value       = aws_sqs_queue.cloudtrail_events.url
}

output "sqs_dlq_url" {
  description = "Dead-letter queue — monitor depth; non-zero = malformed S3 events"
  value       = aws_sqs_queue.cloudtrail_events_dlq.url
}

# ── SSM ───────────────────────────────────────────────────

output "ssm_last_train_ts" {
  value = aws_ssm_parameter.last_train_ts.name
}

# ── IRSA role ARNs ────────────────────────────────────────
# Paste the relevant ARN into the eks.amazonaws.com/role-arn annotation
# on each K8s ServiceAccount manifest.

output "irsa_kserve_sa" {
  value = aws_iam_role.kserve_sa.arn
}

output "irsa_poller_sa" {
  value = aws_iam_role.poller_sa.arn
}

output "irsa_agent_sa" {
  value = aws_iam_role.agent_sa.arn
}

output "irsa_kfp_pipeline_sa" {
  value = aws_iam_role.kfp_pipeline.arn
}

# ── ECR ───────────────────────────────────────────────────

output "ecr_agent_url" {
  value = aws_ecr_repository.agent.repository_url
}

output "ecr_poller_url" {
  value = aws_ecr_repository.poller.repository_url
}

output "ecr_training_url" {
  value = aws_ecr_repository.training_pipeline.repository_url
}

# ── GitHub Actions IAM roles ──────────────────────────────

output "github_terraform_role_arn" {
  description = "Set AWS_984445750473 in GitHub Secrets; role ARN is derived in workflow"
  value       = aws_iam_role.github_terraform.arn
}

output "github_ecr_role_arn" {
  value = aws_iam_role.github_ecr.arn
}

# ── Bastion ───────────────────────────────────────────────

output "bastion_ssh" {
  value = "ssh -i ~/.ssh/${var.bastion_key_pair}.pem ec2-user@${aws_instance.bastion.public_ip}"
}
