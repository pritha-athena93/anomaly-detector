variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name — prefix for all AWS resource names"
  type        = string
  default     = "anomaly-detector"
}

variable "bastion_key_pair" {
  description = "EC2 key pair name for bastion SSH access"
  type        = string
  default     = "bastion-key"
}

variable "grafana_admin_password" {
  description = "Admin password for Grafana"
  type        = string
  sensitive   = true
}
