# AWS bench layer: the billable instances, one per name in `vms`, in the bucket's zone, with the
# infra layer's role, key and security group. `down`/`up` churn only these; the bucket and the
# network persist.

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

# AMD Zen 4, as the laptop the twins ran on is: the twins' predictions price CPU at the laptop's
# pace. 8 vCPUs and 16 GiB.
variable "instance_type" {
  type    = string
  default = "c7a.2xlarge"
}

# The VMs, by name: one that runs every configuration in turn, or one per configuration (deploy.sh,
# FLEET). Adding a name leaves the others running.
variable "vms" {
  type    = list(string)
  default = []
}

variable "ssh_user" {
  type    = string
  default = "ubuntu"
}

# The fork's build, Gradle's and Maven's caches and the results; the image's 8 GiB do not hold them.
variable "disk_gb" {
  type    = number
  default = 64
}

data "terraform_remote_state" "infra" {
  backend = "local"
  config = {
    path = "${path.module}/../infra/terraform.tfstate"
  }
}

# Ubuntu 24.04, whose OpenJDK 17 package the twins ran on.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"]

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "bench" {
  for_each                    = toset(var.vms)
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = data.terraform_remote_state.infra.outputs.subnet_id
  vpc_security_group_ids      = [data.terraform_remote_state.infra.outputs.security_group_id]
  iam_instance_profile        = data.terraform_remote_state.infra.outputs.instance_profile
  key_name                    = data.terraform_remote_state.infra.outputs.key_name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/../../cloud-init.sh")

  root_block_device {
    volume_size = var.disk_gb
    volume_type = "gp3"
  }

  metadata_options {
    http_tokens = "required"
  }

  tags = { Project = "rmow-bench", Name = "rmow-bench-${each.key}" }

  # A VM, once up, changes only by going down: an apply that adds another must not replace or
  # restart it mid-run for a newer image, an edited cloud-init.sh or another instance type
  lifecycle {
    ignore_changes = all
  }
}

# Normalized outputs, the same on both clouds (deploy.sh depends only on these).
output "public_ips" {
  value = { for name, vm in aws_instance.bench : name => vm.public_ip }
}

output "ssh_user" {
  value = var.ssh_user
}
