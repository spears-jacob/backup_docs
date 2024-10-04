project_tag = "pi-qtm-dasp"
environment = "prod"
region = "us-east-1"
job_type = "d-qty"
aws_account_id = "387455165365"
enable_schedule = true
schedule_expression = "cron(15 12 * * ? *)"
coordinator_job_tags = {
  Org = "digital-platforms",
  Group = "digital-insights",
  Team = "self-service-platforms-reporting",
  Solution = "sspp",
  Tech = "hive",
  Stack = "hive"
  App = "daily-sspp-quality-coordinator"
  Mission-critical = "yes"
}
