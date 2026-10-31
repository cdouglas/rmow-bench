# Azure bench layer: the billable VMs (each with its public IP, NIC and disk), one per name in
# `vms`, in the infra layer's subnet, bound to its NSG, with its identity. `down`/`up` churn only
# these; the account and the network persist.

terraform {
  required_version = ">= 1.3"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# AMD Zen 4, as the laptop the twins ran on is and the AWS c7a: the twins' predictions price CPU
# at the laptop's pace. 8 vCPUs and 32 GiB.
variable "vm_size" {
  type    = string
  default = "Standard_D8as_v6"
}

# The VMs, by name: one that runs every configuration in turn, or one per configuration (deploy.sh,
# FLEET). Adding a name leaves the others running.
variable "vms" {
  type    = list(string)
  default = []
}

variable "admin_username" {
  type    = string
  default = "ubuntu"
}

# Azure takes only RSA keys here (admin_ssh_key rejects ed25519).
variable "ssh_pub_key" {
  type        = string
  description = "Path to the operator's SSH public key (RSA)."
}

# The fork's build, Gradle's and Maven's caches and the results.
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

locals {
  infra = data.terraform_remote_state.infra.outputs
}

resource "azurerm_public_ip" "pip" {
  for_each            = toset(var.vms)
  name                = "rmow-bench-${each.key}-pip"
  location            = local.infra.location
  resource_group_name = local.infra.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "nic" {
  for_each            = toset(var.vms)
  name                = "rmow-bench-${each.key}-nic"
  location            = local.infra.location
  resource_group_name = local.infra.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = local.infra.subnet_id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.pip[each.key].id
  }
}

resource "azurerm_network_interface_security_group_association" "nic_nsg" {
  for_each                  = toset(var.vms)
  network_interface_id      = azurerm_network_interface.nic[each.key].id
  network_security_group_id = local.infra.nsg_id
}

resource "azurerm_linux_virtual_machine" "vm" {
  for_each              = toset(var.vms)
  name                  = "rmow-bench-${each.key}"
  resource_group_name   = local.infra.resource_group_name
  location              = local.infra.location
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.nic[each.key].id]
  custom_data           = filebase64("${path.module}/../../cloud-init.sh")

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(var.ssh_pub_key)
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [local.infra.identity_id]
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = var.disk_gb
  }

  # Ubuntu 24.04, whose OpenJDK 17 package the twins ran on.
  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  # A VM, once up, changes only by going down: an apply that adds another must not replace or
  # restart it mid-run for an edited cloud-init.sh or another size
  lifecycle {
    ignore_changes = all
  }
}

# Normalized outputs, the same on both clouds (deploy.sh depends only on these).
output "public_ips" {
  value = { for name, ip in azurerm_public_ip.pip : name => ip.ip_address }
}

output "ssh_user" {
  value = var.admin_username
}
