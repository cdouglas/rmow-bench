# AWS infra layer: the store, the instance's credentials and the network, free while the bucket is
# empty. A directory bucket in one Availability Zone, a role that may open sessions on it and
# nothing else, the operator's SSH key, a security group admitting SSH from the operator alone, and
# the gateway endpoint AWS recommends for reaching S3 Express from a VPC. Apply once (`deploy.sh
# infra-up`); `teardown` deletes the bucket with every object in it.

terraform {
  required_version = ">= 1.3"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "az_id" {
  type        = string
  default     = "use1-az4"
  description = "The bucket's and the instance's zone, by ID (names map to IDs per account)."
}

# No default: deploy.sh passes the operator's detected IP/32, and there is deliberately no
# world-open fallback.
variable "ssh_ingress_cidr" {
  type        = string
  description = "CIDR allowed to SSH in (operator IP/32)."
}

variable "ssh_pub_key" {
  type        = string
  description = "Path to the operator's SSH public key."
}

data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  default = true
}

# The default VPC's subnet in the bucket's zone, so the instance measures the store, not the
# network.
data "aws_subnet" "zone" {
  vpc_id               = data.aws_vpc.default.id
  availability_zone_id = var.az_id
  default_for_az       = true
}

data "aws_route_table" "main" {
  vpc_id = data.aws_vpc.default.id
  filter {
    name   = "association.main"
    values = ["true"]
  }
}

locals {
  tags = { Project = "rmow-bench" }
}

# Directory bucket names are unique within their zone across accounts: the account's ID makes this
# one the operator's.
resource "aws_s3_directory_bucket" "bench" {
  bucket        = "rmow-bench-${data.aws_caller_identity.current.account_id}--${var.az_id}--x-s3"
  force_destroy = true

  location {
    name = var.az_id
  }
}

resource "aws_vpc_endpoint" "s3express" {
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3express"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [data.aws_route_table.main.id]
  tags              = local.tags
}

resource "aws_iam_role" "bench" {
  name_prefix = "rmow-bench-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.tags
}

# A directory bucket authorizes every object request through a session: CreateSession on the
# bucket is the whole of the instance's access.
resource "aws_iam_role_policy" "bucket" {
  name = "rmow-bench-bucket"
  role = aws_iam_role.bench.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3express:CreateSession"
      Resource = aws_s3_directory_bucket.bench.arn
    }]
  })
}

resource "aws_iam_instance_profile" "bench" {
  name_prefix = "rmow-bench-"
  role        = aws_iam_role.bench.name
  tags        = local.tags
}

resource "aws_key_pair" "operator" {
  key_name_prefix = "rmow-bench-"
  public_key      = file(var.ssh_pub_key)
  tags            = local.tags
}

resource "aws_security_group" "bench" {
  name_prefix = "rmow-bench-"
  description = "rmow-bench VM: SSH in from the operator, all egress."
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_ingress_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.tags
}

output "subnet_id" {
  value = data.aws_subnet.zone.id
}

output "security_group_id" {
  value = aws_security_group.bench.id
}

output "instance_profile" {
  value = aws_iam_instance_profile.bench.name
}

output "key_name" {
  value = aws_key_pair.operator.key_name
}

output "bucket" {
  value = aws_s3_directory_bucket.bench.bucket
}

output "region" {
  value = var.region
}

# The runs' root: the tests, the spike and the calibration each write under a prefix of it.
output "root" {
  value = "s3://${aws_s3_directory_bucket.bench.bucket}/rmow-bench"
}
