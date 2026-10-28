# Azure infra layer: the store, the VM's identity and the network, free while the account is empty.
# A StorageV2 account with a hierarchical namespace (ADLS Gen2) and a container, a user-assigned
# identity with Storage Blob Data Contributor on the account, and a resource group, vnet, subnet
# and NSG admitting SSH from the operator alone. Apply once (`deploy.sh infra-up`); `teardown`
# deletes the resource group, the account and every blob in it with it.

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

variable "location" {
  type    = string
  default = "eastus"
}

variable "resource_group_name" {
  type    = string
  default = "rmow-bench"
}

# No world-open default: deploy.sh passes the operator's detected IP/32.
variable "source_address_prefix" {
  type        = string
  description = "CIDR allowed to SSH in."
}

data "azurerm_client_config" "current" {}

locals {
  # Account names are global: the subscription's hash makes this one the operator's.
  account = "rmowbench${substr(md5(data.azurerm_client_config.current.subscription_id), 0, 8)}"
}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "vnet" {
  name                = "rmow-bench-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

# The storage service endpoint keeps the VM's requests to the account on Azure's backbone.
resource "azurerm_subnet" "subnet" {
  name                 = "rmow-bench-subnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
  service_endpoints    = ["Microsoft.Storage"]
}

resource "azurerm_network_security_group" "nsg" {
  name                = "rmow-bench-nsg"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.source_address_prefix
    destination_address_prefix = "*"
  }
}

resource "azurerm_storage_account" "bench" {
  name                     = local.account
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_kind             = "StorageV2"
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier              = "Hot"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
}

resource "azurerm_storage_container" "bench" {
  name                 = "rmow-bench"
  storage_account_name = azurerm_storage_account.bench.name
}

# The VM's credentials: the SDK's default chain finds the identity, and no configuration holds a
# key. Assigning the role needs Owner or User Access Administrator on the subscription.
resource "azurerm_user_assigned_identity" "bench" {
  name                = "rmow-bench"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_role_assignment" "bench" {
  scope                = azurerm_storage_account.bench.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.bench.principal_id
}

output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "location" {
  value = azurerm_resource_group.rg.location
}

output "subnet_id" {
  value = azurerm_subnet.subnet.id
}

output "nsg_id" {
  value = azurerm_network_security_group.nsg.id
}

output "identity_id" {
  value = azurerm_user_assigned_identity.bench.id
}

# Not a secret: it names the identity the SDK should ask the VM's metadata service for.
output "client_id" {
  value = azurerm_user_assigned_identity.bench.client_id
}

output "account" {
  value = azurerm_storage_account.bench.name
}

output "container" {
  value = azurerm_storage_container.bench.name
}

# The runs' root: the tests, the spike and the calibration each write under a prefix of it.
output "root" {
  value = "abfss://${azurerm_storage_container.bench.name}@${azurerm_storage_account.bench.name}.dfs.core.windows.net/rmow-bench"
}
