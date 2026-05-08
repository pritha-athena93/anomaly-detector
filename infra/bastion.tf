# ── Bastion Host ──────────────────────────────────────────
# Single t3.micro EC2 in a public subnet.
# Purpose:
#   - kubectl / helm access to the private EKS API endpoint
#   - ArgoCD Application bootstrap
#   - Debugging pods via cluster-internal DNS
#
# Access:
#   ssh -i ~/.ssh/<bastion_key_pair>.pem ec2-user@<bastion_public_ip>
#   (bastion_public_ip output from terraform output)
#
# The startup script installs kubectl + aws CLI, then runs
# aws eks update-kubeconfig so kubectl works immediately on SSH.

# ── Key Pair variable ─────────────────────────────────────
# Create the key pair in AWS Console or via `aws ec2 create-key-pair`,
# then pass the name here via TF_VAR_bastion_key_pair or terraform.tfvars.

# ── AMI ───────────────────────────────────────────────────

data "aws_ami" "amazon_linux" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

# ── Security Group ────────────────────────────────────────

resource "aws_security_group" "bastion" {
  name        = "${var.cluster_name}-bastion"
  description = "Bastion host SSH inbound, all outbound"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "restrict to your IP in prod"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.cluster_name}-bastion" }
}

# ── IAM Role for Bastion ──────────────────────────────────

resource "aws_iam_role" "bastion" {
  name = "${var.cluster_name}-bastion"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "bastion_eks" {
  name = "eks-access"
  role = aws_iam_role.bastion.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster", "eks:ListClusters"]
        Resource = "*"
      },
      {
        # Required for: aws eks update-kubeconfig to generate a token
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster"]
        Resource = "*"
      },
      {
        # setup-rds-schema.sh fetches the DSN from Secrets Manager
        Effect   = "Allow"
        Action   = ["secretsmanager:GetSecretValue"]
        Resource = [
          "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:anomaly/*"
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "bastion" {
  name = "${var.cluster_name}-bastion"
  role = aws_iam_role.bastion.name
}

# ── EC2 Instance ──────────────────────────────────────────

resource "aws_instance" "bastion" {
  ami                         = data.aws_ami.amazon_linux.id
  instance_type               = "t3.micro"
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.bastion.id]
  iam_instance_profile        = aws_iam_instance_profile.bastion.name
  key_name                    = var.bastion_key_pair
  associate_public_ip_address = true

  user_data = <<-EOT
    #!/bin/bash
    yum update -y
    # kubectl
    curl -LO "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    chmod +x kubectl && mv kubectl /usr/local/bin/
    # helm
    curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
    # kubeconfig for the EKS cluster (requires the bastion IAM role to have eks:DescribeCluster)
    aws eks update-kubeconfig --region ${var.region} --name ${var.cluster_name}
  EOT

  tags = { Name = "${var.cluster_name}-bastion" }

  depends_on = [aws_eks_cluster.primary]
}
