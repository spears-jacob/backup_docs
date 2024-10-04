module "secrets_create_and_call"{
  source               = "../../../../secret-template"
  enable_secret        =  true
  secret_name          = "ancientftpcreds"
  secret_description   = "credentials from a long time ago, great for testing"
  secret_string        = {"user":"${var.omniture_old_for_testing_username}","pass":"${var.omniture_old_for_testing_password}"}
  secrets_manager_tags = local.job_tags
  flow_category        = "dasp"
  environment          = "stg"
}
