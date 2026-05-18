terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Environment = var.environment
      Project     = var.project
      ManagedBy   = "terraform"
    }
  }
}

module "vpc" {
  source      = "./modules/vpc"
  environment = var.environment
  project     = var.project
  vpc_cidr    = var.vpc_cidr
}

module "eks" {
  source                = "./modules/eks"
  environment           = var.environment
  project               = var.project
  vpc_id                = module.vpc.vpc_id
  private_subnet_ids    = module.vpc.private_subnet_ids
  node_instance_types   = var.eks_node_instance_types
  node_desired_capacity = var.eks_desired_capacity
}

module "msk" {
  source             = "./modules/msk"
  environment        = var.environment
  project            = var.project
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  instance_type      = var.msk_instance_type
  broker_nodes       = var.msk_broker_nodes
}

module "s3_iceberg" {
  source      = "./modules/s3_iceberg"
  environment = var.environment
  project     = var.project
}

module "trino" {
  source             = "./modules/trino"
  environment        = var.environment
  project            = var.project
  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids
  eks_cluster_name   = module.eks.cluster_name
}
