project_tag = "pi-qtm-dasp"
environment = "dev"
region = "us-west-2"
job_type = "m"
aws_account_id = "213705006773"
enable_schedule = false
schedule_expression = "cron(0 17 29-31,1-2 * ? *)"
coordinator_job_tags = {
  Org = "digital-platforms",
  Group = "digital-insights",
  Team = "self-service-platforms-reporting",
  Solution = "sspp",
  Tech = "hive",
  Stack = "hive"
  App = "monthly-sspp-coordinator"
  Mission-critical = "no"
}
