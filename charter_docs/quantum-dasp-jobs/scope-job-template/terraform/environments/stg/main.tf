locals {
  env = "stg"

  read_buckets_locations = data.null_data_source.read_buckets_locations.*.outputs.value
  read_s3_locations = concat(local.read_buckets_locations, [
    "s3://charter-aws-dl-poc-sharebucket/",
    "s3://pi-bi-red-stg-feeds-pii/"
]
  )

  read_s3_locations_with_prod_buckets = concat(
    local.read_s3_locations,
    [
      "s3://com.charter.focus.prod.export.files/quantum-list-match/",
      "s3://pi-bi-red-prod-feeds-pii/",
      "s3://pi-bi-red-stg-feeds-pii/",
      "s3://pi-ci-wifi-prod-aggregates-nopii/",
      "s3://pi-ci-wifi-prod-feeds/",
      "s3://pi-ci-wifi-stg-aggregates-nopii/",
      "s3://pi-ci-wifi-stg-feeds/",
      "s3://pi-global-sec-repo-stg-feeds-pii/data/stg_sec_repo_sspp/",
      "s3://pi-global-stg-udf-jars/",
      "s3://pi-global-stg-udf-keys/",
      "s3://pi-qtm-cs-prod-events-pii/",
      "s3://pi-qtm-cs-stg-events-pii/",
      "s3://pi-qtm-dasp-prod-aggregates-nopii/data/prod_dasp/asp_asapp_convos_metadata/",
      "s3://pi-qtm-dasp-prod-aggregates-pii/data/incoming/mvno/mvno_accounts",
      "s3://pi-qtm-dasp-prod-aggregates-pii/data/prod_dasp/quantum_metric_agg_portals/",
      "s3://pi-qtm-dasp-stg-aggregates-nopii/",
      "s3://pi-qtm-dasp-stg-aggregates-pii/",
      "s3://pi-qtm-exp-prod-joining-keys-aggregates-pii/data/prod/experiment_btm_visit_id_account_key_lookup",
      "s3://pi-qtm-global-prod-core-events-cleansed/",
      "s3://pi-qtm-global-prod-sspp/",
      "s3://pi-qtm-si-*/",
      "s3://pi-qtm-si-stg-aggregates-pii/",
      "s3://pi-qtm-si-stg-events-pii/",
      "s3://pi-swe-iden-prod-events-pii/",
      "s3://pi-swe-iden-stg-events-pii/",
      "s3://pi-wireless-mob-prod-feeds-pii/data/prod_mob/mvno_smdh_accounts"
    ]
  )
  #todo: remove hardcoded "pi-qtm-dasp-prod-aggregates-nopii" bucket from the list

  write_buckets_locations = data.null_data_source.write_buckets_locations.*.outputs.value
  write_s3_locations = concat(
  local.write_buckets_locations,
    [
      "s3://213705006773-us-east-1-stg-workgroup-athena-results-bucket/",
      "s3://aws-athena-query-results-387455165365-us-east-1/",
      "s3://com.charter.focus.prod.ingest.files/",
      "s3://pi-global-sec-repo-stg-feeds-pii/data/stg_sec_repo_sspp/",
      "s3://pi-qtm-cs-stg-aggregates-nopii/",
      "s3://pi-qtm-dasp-prod-aggregates-nopii/data/prod_dasp/nifi/app_figures/",
      "s3://pi-qtm-dasp-stg-aggregates-nopii/",
      "s3://pi-qtm-dasp-stg-aggregates-pii/",
      "s3://pi-qtm-si-stg-aggregates-nopii/",
      "s3://pi-qtm-si-stg-aggregates-pii/"
    ]
  )
  common_tags = {
    stream-category = var.stream_category
    environment = local.env
    Env = "stage"
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
  environment   = "stg"
  business_unit = var.business_unit

  config_output_file = var.config_output_file

  artifacts_dir    = var.artifacts_dir
  s3_artifacts_dir = var.s3_artifacts_dir

  launch_script_version = var.launch_script_version
  config_file_version   = var.config_file_version
  job_version           = var.job_version

  step_scripts = var.step_scripts

  project_tag         = "charter-dl"
  sfn_emr_temp_runner = "arn:aws:states:us-east-1:213705006773:stateMachine:pi-qtm-global-stg-sfn-emr-temp-runner"
  job_emr_runner = "arn:aws:states:us-east-1:213705006773:stateMachine:pi-qtm-global-stg-job-emr-runner"

  schedule_expression = var.schedule_expression
  enable_schedule     = var.enable_schedule

  default_target_capacity    = var.default_target_capacity
  master_instance_types      = var.master_instance_types
  worker_instance_types      = var.worker_instance_types
  price_as_percentage        = var.price_as_percentage
  emr_security_configuration = "pi-qtm-global-stg-security-configuration"
  core_ebs_volume_size_in_gb = var.core_ebs_volume_size_in_gb

  tez_memory_mb              = var.tez_memory_mb
  spark_packages             = "org.apache.spark:spark-avro_2.11:2.4.3"
  glue_catalog_separator     = var.glue_catalog_separator

  allow_parallel_execution   = var.allow_parallel_execution
  emr_version                 = var.emr_version

  allocation_strategy    = var.allocation_strategy
  block_duration_minutes = var.block_duration_minutes

  write_kms_key    = var.write_kms_key
  read_kms_keys = [
    "arn:aws:kms:us-east-1:213705006773:key/41e24499-e622-4796-98ad-9ffaba7d3bef",
    "arn:aws:kms:us-east-1:387455165365:key/c226775d-0b8a-41a1-89bf-e5e7e361181f",
    "arn:aws:kms:us-east-1:213705006773:key/706750ba-9432-4b10-88b6-d42fbd9c6560",
    "arn:aws:kms:us-east-1:213705006773:key/ae95b022-2f74-4d02-b91f-85f8f0516910"
  ]

  vpc = "vpc-0f84fe5bab496efea"
  subnets = [
      "subnet-04a800c1114a25bc7",
      "subnet-05f684264fcc8e2ce",
      "subnet-070ee3dbb88215965",
      "subnet-0a4e9cd3687eee789",
      "subnet-0bb31d029d4b5edbb",
      "subnet-0dfb20bf646b4d522"
  ]

  notification_email = "C-Mrdudula.Geddam@charter.com"
  flow_category      = var.flow_category
  job_name           = var.job_name
  stream_category    = var.stream_category

  read_db_table_names             = var.read_db_table_names
  write_db_table_names            = var.write_db_table_names
  sns_https_notification_endpoint = var.sns_https_notification_endpoint
  #Populate from CI/CD by defining value as enviornment variable TF_VAR_sns_https_notification_endpoint to avoid putting credentials in repo.

  artifacts_bucket   = module.global_bucket_names_constructor.constructed_s3_buckets_names[0]
  logs_bucket        = module.global_bucket_names_constructor.constructed_s3_buckets_names[1]
  write_s3_locations = local.write_s3_locations
  read_s3_locations  = local.read_s3_locations_with_prod_buckets

  job_extra_properties = var.extra_properties
  common_extra_properties = local.common_extra_properties

  emr_srv_iam_role_arn = "arn:aws:iam::213705006773:role/pi-qtm-global-stg-cmn-emr-service-role"
  run_job_lambda_iam_role_arn = "arn:aws:iam::213705006773:role/pi-qtm-global-stg-cmn-run-job-role"
}

module "global_bucket_names_constructor" {
  source = "git_at_company_dot_net:awspilot/ddl-managerdot_git//flows/global-db-tables-init/utils/s3-bucket-names-constructor?ref=stg"

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
