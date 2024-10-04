project_tag = "pi-qtm-dasp"
environment = "stg"
region = "us-east-1"
job_type = "d"
aws_account_id = "213705006773"
enable_schedule = true
schedule_expression = "cron(15 10 * * ? *)"
coordinator_job_tags = {
  Org = "digital-platforms",
  Group = "digital-insights",
  Team = "self-service-platforms-reporting",
  Solution = "sspp",
  Tech = "hive",
  Stack = "hive"
  App = "daily-sspp-coordinator"
  Mission-critical = "no"
}
