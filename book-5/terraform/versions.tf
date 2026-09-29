# Written against Terraform 1.5.7 and hashicorp/aws 6.66.0 (the versions
# `terraform validate` was run with, 28 Sep 2026). Never applied: no AWS
# account was used to write Book 5.
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.66"
    }
  }
}

provider "aws" {
  region = var.region
}
