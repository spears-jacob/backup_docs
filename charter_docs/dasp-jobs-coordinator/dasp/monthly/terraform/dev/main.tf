provider "aws" {
  version = "~> 3.13.0"
  region = var.region
}

terraform {
  backend "s3" {
    encrypt = true
    bucket = "charter-telemetry-pilot-tf-state-dev"
    dynamodb_table = "terraform-state-lock-dynamo"
    key = "dasp-monthly-coordinator-dev/terraform.tfstate"
    region = "us-west-2"
  }
}

data "terraform_remote_state" "transient_emr_components" {
  backend = "s3"
  config = {
    encrypt = true
    bucket = "charter-telemetry-pilot-tf-state-dev"
    key    = "charter-telemetry-pilot-dev/terraform.tfstate"
    region = "us-west-2"
  }
}

locals {
  job_tags = [for k,v in var.coordinator_job_tags : {Key = k, Value = v}]
  cleanup_sfn = data.terraform_remote_state.transient_emr_components.outputs.shared_emr_cleanup_sfn
  emr_security_configuration = data.terraform_remote_state.transient_emr_components.outputs.emr_security_configuration
  shared_emr_master_sg_id = data.terraform_remote_state.transient_emr_components.outputs.shared_emr_master_sg_id
  shared_emr_slave_sg_id = data.terraform_remote_state.transient_emr_components.outputs.shared_emr_slave_sg_id
  shared_emr_service_sg_id = data.terraform_remote_state.transient_emr_components.outputs.shared_emr_service_sg_id
  private_subnets = data.terraform_remote_state.transient_emr_components.outputs.private_subnets
}

data "template_file" "coordinator"{
  template = "${file("${path.module}/../../../../py-coordinator/monthly.json")}"
  vars = {
    project_tag = var.project_tag
    environment = var.environment
    job_type = var.job_type
    region = var.region
    aws_account_id = var.aws_account_id
    enable_schedule = var.enable_schedule
    schedule_expression = var.schedule_expression
    logs_bucket_name = "pi-qtm-global-dev-logs"
    emr_security_configuration = local.emr_security_configuration
    master_sg = local.shared_emr_master_sg_id
    slave_sg = local.shared_emr_slave_sg_id
    service_sg = local.shared_emr_service_sg_id
    subnet_ids = jsonencode(local.private_subnets)
    job_tags = jsonencode(local.job_tags)
    tez_memory_mb = "8192"
    spark_packages = ""
  }
}

module "jobs-coordinator" {
  source = "../../../../coordinator-template/terraform/modules/coordinator_template"
  project_tag = var.project_tag
  environment = var.environment
  job_type = var.job_type
  region = var.region
  aws_account_id = var.aws_account_id
  enable_schedule = var.enable_schedule
  schedule_expression = var.schedule_expression
  definition = data.template_file.coordinator.rendered
  coordinator_job_tags = var.coordinator_job_tags
  cleanup_sfn_arn = local.cleanup_sfn
}
