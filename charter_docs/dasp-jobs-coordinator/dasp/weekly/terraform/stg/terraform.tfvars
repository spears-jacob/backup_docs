project_tag = "pi-qtm-dasp"
environment = "stg"
region = "us-east-1"
job_type = "w"
aws_account_id = "213705006773"
enable_schedule = false
schedule_expression = "cron(0 17 ? * SAT *)"
coordinator_job_tags = {
  Org = "digital-platforms",
  Group = "digital-insights",
  Team = "self-service-platforms-reporting",
  Solution = "sspp",
  Tech = "hive",
  Stack = "hive"
  App = "weekly-sspp-coordinator"
  Mission-critical = "no"
}
