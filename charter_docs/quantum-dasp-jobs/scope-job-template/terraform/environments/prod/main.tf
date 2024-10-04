locals {
  env = "prod"

  read_buckets_locations = data.null_data_source.read_buckets_locations.*.outputs.value
  read_s3_locations = concat(local.read_buckets_locations, [
    "s3://charter-aws-dl-poc-sharebucket/",
    "s3://com.charter.focus.prod.export.files/quantum-list-match/",
    "s3://com.charter.sia.prod.export.files/quantum-list-match/",
    "s3://pi-bi-red-prod-feeds-pii/",
    "s3://pi-ci-wifi-prod-aggregates-nopii/",
    "s3://pi-ci-wifi-prod-feeds/",
    "s3://pi-ci-wifi-stg-aggregates-nopii/",
    "s3://pi-global-prod-udf-jars/",
    "s3://pi-global-prod-udf-keys/",
    "s3://pi-qtm-dasp-prod-aggregates-nopii/data/prod_dasp/asp_asapp_convos_metadata/",
    "s3://pi-qtm-dasp-prod-aggregates-pii/data/incoming/mvno/mvno_accounts",
    "s3://pi-qtm-exp-prod-joining-keys-aggregates-pii/data/prod/experiment_btm_visit_id_account_key_lookup",
    "s3://pi-qtm-global-prod-core-events-cleansed/",
    "s3://pi-qtm-global-prod-sspp/",
    "s3://pi-qtm-si-prod-events-pii/",
    "s3://pi-swe-iden-prod-events-pii/",
    "s3://pi-wireless-mob-prod-feeds-pii/data/prod_mob/mvno_smdh_accounts"
  ]
  )
  #todo: remove hardcoded "pi-qtm-dasp-prod-aggregates-nopii" bucket from the list

  write_buckets_locations = data.null_data_source.write_buckets_locations.*.outputs.value
  write_s3_locations = concat(local.write_buckets_locations, [
    "s3://387455165365-us-east-1-prod-workgroup-athena-results-bucket/pi-qtm-adhoc/output/",
    "s3://aws-athena-query-results-387455165365-us-east-1/",
    "s3://com.charter.focus.prod.ingest.files/",
    "s3://pi-global-sec-repo-prod-feeds-pii/data/prod_sec_repo_sspp/",
    "s3://pi-qtm-dasp-prod-aggregates-nopii/data/prod_dasp/nifi/app_figures/",
    "s3://pi-qtm-si-prod-aggregates-nopii/",
    "s3://pi-qtm-si-prod-aggregates-pii/"
  ]

  )
  common_tags = {
    stream-category = var.stream_category
    environment = local.env
    Env = "production"
    job-name = var.job_name
    flow-category = var.flow_category
    Org              = "digital-platforms"
    Group            = "digital-insights"
    Team             = "self-service-platforms-reporting"
    Solution         = "sspp"
  }
  common_extra_properties = {}
}

module "job" {
  source = "../../../../job-template/terraform/modules/job-template"
  job_tags      = var.job_tags
  common_tags = local.common_tags
  region        = "us-east-1"
  region_abbr   = "ue1"
  environment   = local.env
  business_unit = var.business_unit

  config_output_file = var.config_output_file

  artifacts_dir    = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir

  launch_script_version = var.launch_script_version
  config_file_version   = var.config_file_version
  job_version           = var.job_version

  step_scripts = var.step_scripts

  project_tag         = "charter-dl"
  sfn_emr_temp_runner = "arn:aws:states:us-east-1:387455165365:stateMachine:pi-qtm-global-prod-sfn-emr-temp-runner"
  job_emr_runner = "arn:aws:states:us-east-1:387455165365:stateMachine:pi-qtm-global-prod-job-emr-runner"

  schedule_expression = var.schedule_expression
  enable_schedule     = var.enable_schedule

  default_target_capacity    = var.default_target_capacity
  master_instance_types      = var.master_instance_types
  worker_instance_types      = var.worker_instance_types
  price_as_percentage        = var.price_as_percentage
  emr_security_configuration = "pi-qtm-global-prod-security-configuration"
  emr_version = var.emr_version
  core_ebs_volume_size_in_gb = var.core_ebs_volume_size_in_gb

  tez_memory_mb              = var.tez_memory_mb
  spark_packages             = "org.apache.spark:spark-avro_2.11:2.4.3"
  glue_catalog_separator = var.glue_catalog_separator

  allow_parallel_execution = var.allow_parallel_execution

  allocation_strategy    = var.allocation_strategy
  block_duration_minutes = var.block_duration_minutes

  write_kms_key = var.write_kms_key
  read_kms_keys = [
    "arn:aws:kms:us-east-1:387455165365:key/c226775d-0b8a-41a1-89bf-e5e7e361181f",
    "arn:aws:kms:us-east-1:387455165365:key/8309e3ba-62db-4597-acf3-7061e31871ef",
    "arn:aws:kms:us-east-1:387455165365:key/73372433-3357-42be-b29f-03c4a27eedc9",
    "arn:aws:kms:us-east-1:387455165365:key/d68e75a8-e9b7-4557-af3e-16967d19bf47",
    "arn:aws:kms:us-east-1:213705006773:key/41e24499-e622-4796-98ad-9ffaba7d3bef",
  ]

  vpc = "vpc-0b21c2e48264910f4"
  subnets = [
    "subnet-02dcc6ecdb28311ba",
    "subnet-08e7efe95d2bed1c2",
    "subnet-015e68315a0a09917",
    "subnet-07ca271203f71058f",
    "subnet-0dae4ba98f423a869"
  ]


  notification_email = "C-Josh.Jones@charter.com"
  flow_category      = var.flow_category
  job_name           = var.job_name
  stream_category    = var.stream_category

  read_db_table_names  = var.read_db_table_names
  write_db_table_names = var.write_db_table_names
  # Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.
  sns_https_notification_endpoint = var.sns_https_notification_endpoint

  artifacts_bucket = module.global_bucket_names_constructor.constructed_s3_buckets_names[0]
  logs_bucket      = module.global_bucket_names_constructor.constructed_s3_buckets_names[1]

  write_s3_locations = local.write_s3_locations
  read_s3_locations  = local.read_s3_locations

  job_extra_properties = var.extra_properties
  common_extra_properties = local.common_extra_properties

  emr_srv_iam_role_arn = "arn:aws:iam::387455165365:role/pi-qtm-global-prod-cmn-emr-service-role"
  run_job_lambda_iam_role_arn = "arn:aws:iam::387455165365:role/pi-qtm-global-prod-cmn-run-job-role"
}

module "global_bucket_names_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-managerdot_git//flows/global-db-tables-init/utils/s3-bucket-names-constructor?ref=master"

  env             = local.env
  business_unit   = var.business_unit
  stream_category = var.stream_category

  s3_buckets_keys = [
    "artifacts",
    "logs"
  ]
}

data "null_data_source" "write_buckets_locations" {
  count = length(var.write_db_table_names)
  inputs = {
    value = lookup(local.all_db_table_name_to_s3_info_map, var.write_db_table_names[count.index]).s3_location
  }
}

data "null_data_source" "read_buckets_locations" {
  count = length(var.read_db_table_names)
  inputs = {
    value = lookup(local.all_db_table_name_to_s3_info_map, var.read_db_table_names[count.index]).s3_location
  }
}

data "null_data_source" "s3_buckets_access"{
  count = length(var.s3_buckets)
  inputs = {
    value = var.s3_buckets[count.index]
  }
}
