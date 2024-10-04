### set the Org, Group, and team tags to correspond with the appropriate group ###
locals {
secret_tags = {
    Org              = "digital-platforms"
    Group            = "digital-insights"
    Team             = "self-service-platforms-reporting"
    Mission-critical = "no"
    Component        = "terraform"
    Solution         = "secrets"
    Job-name         =  var.secret_name
  }
  common_tags = merge(local.secret_tags, var.secrets_manager_tags)

}

module "scope_job_secret_call"{
  source               =  "../job-template/terraform/modules/secrets-template"
  enable_secret        = var.enable_secret
  secret_name          = var.secret_name
  secret_description   = var.secret_description
  secret_string        = var.secret_string
  secrets_manager_tags = local.common_tags
  flow_category        = var.flow_category
  environment          = var.environment
}
