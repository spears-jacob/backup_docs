variable secret_string {
  description = "This variable holds the secret values"
}

variable "secret_description" {
  default = "hide your secrets"
}

variable "enable_secret" {
  default = false
}

variable "secret_name" {
  default = "test-secret-name"
}

### Adjust below variables and tags to correspond to the appropriate group ###
variable "flow_category" {
  default = "dasp"
}

### set the Org, Group, Team, and Solution tags to correspond with the appropriate group ###
variable "secrets_manager_tags" {
  default = {
    Org              = "digital-platforms"
    Group            = "digital-insights"
    Team             = "self-service-platforms-reporting"
    Env              =  ""
    Mission-critical = "no"
    Component        = "job"
    Job-name         = "secret-test"
    Solution         = "sspp"
  }
  type = map
}

variable "environment" {
  
}