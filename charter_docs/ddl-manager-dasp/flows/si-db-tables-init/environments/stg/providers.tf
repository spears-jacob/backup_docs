terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.71"
    }
  }
  backend "s3" {}
  required_version = "~> 0.12.0"
}
