# AWS infra layer: the store, the instance's credentials and the network, free while the bucket is
# empty. A directory bucket in one Availability Zone, a role that may open sessions on it and
# nothing else, the operator's SSH key, a security group admitting SSH from the operator alone, and
# the gateway endpoint AWS recommends for reaching S3 Express from a VPC. Apply once (`deploy.sh
# infra-up`); `teardown` deletes the bucket with every object in it.
#
# An operator who may not create IAM roles names an existing bucket and an instance profile whose
# role may open sessions on it (`bucket`, `instance_profile`), and none is made; `teardown` then
# leaves the bucket, and deploy.sh deletes the runs' prefix in it. Where the default VPC has its S3
# Express endpoint already, `create_endpoint = false`.

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
  default = "us-west-2"
}

variable "az_id" {
  type        = string
  default     = "usw2-az3"
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

variable "bucket" {
  type        = string
  default     = ""
  description = "An existing directory bucket in az_id to run under; empty makes one."
}

variable "instance_profile" {
  type        = string
  default     = ""
  description = "An existing instance profile whose role may open sessions on the bucket; empty makes one."
}

variable "create_endpoint" {
  type        = bool
  default     = true
  description = "Make the S3 Express gateway endpoint; false where the default VPC has one."
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

  lifecycle {
    precondition {
      condition     = var.bucket == "" || endswith(var.bucket, "--${var.az_id}--x-s3")
      error_message = "The bucket must be in az_id's zone: its name ends --<az_id>--x-s3."
    }
  }
}

data "aws_route_table" "main" {
  vpc_id = data.aws_vpc.default.id
  filter {
    name   = "association.main"
    values = ["true"]
  }
}

locals {
  tags             = { Project = "rmow-bench" }
  bucket           = coalesce(var.bucket, one(aws_s3_directory_bucket.bench[*].bucket))
  bucket_arn       = "arn:aws:s3express:${var.region}:${data.aws_caller_identity.current.account_id}:bucket/${local.bucket}"
  instance_profile = coalesce(var.instance_profile, one(aws_iam_instance_profile.bench[*].name))
}

# Directory bucket names are unique within their zone across accounts: the account's ID makes this
# one the operator's.
resource "aws_s3_directory_bucket" "bench" {
  count         = var.bucket == "" ? 1 : 0
  bucket        = "rmow-bench-${data.aws_caller_identity.current.account_id}--${var.az_id}--x-s3"
  force_destroy = true

  location {
    name = var.az_id
  }
}

resource "aws_vpc_endpoint" "s3express" {
  count             = var.create_endpoint ? 1 : 0
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3express"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [data.aws_route_table.main.id]
  tags              = local.tags
}

# The role and its profile are untagged: tagging them takes iam:TagRole and iam:TagInstanceProfile,
# which a least-privileged operator (casuser) lacks.
resource "aws_iam_role" "bench" {
  count       = var.instance_profile == "" ? 1 : 0
  name_prefix = "rmow-bench-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# A directory bucket authorizes every object request through a session: CreateSession on the
# bucket is the whole of the instance's access.
resource "aws_iam_role_policy" "bucket" {
  count = length(aws_iam_role.bench)
  name  = "rmow-bench-bucket"
  role  = aws_iam_role.bench[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "s3express:CreateSession"
      Resource = local.bucket_arn
    }]
  })
}

resource "aws_iam_instance_profile" "bench" {
  count       = length(aws_iam_role.bench)
  name_prefix = "rmow-bench-"
  role        = aws_iam_role.bench[0].name
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
  value = local.instance_profile
}

output "key_name" {
  value = aws_key_pair.operator.key_name
}

output "bucket" {
  value = local.bucket
}

output "region" {
  value = var.region
}

# The runs' root: the tests, the spike and the calibration each write under a prefix of it.
output "root" {
  value = "s3://${local.bucket}/rmow-bench"
}
