locals {
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
}

module "lambda-sfn-adapter" {
  source = "git_at_company_dot_net:awspilot/charter-telemetry-pilot-tfmodulesdot_git//lambda-sfn-adapter?ref=feature/sfn-adapter-path-update-for-jobs-coordinator"
  environment = var.environment
  project_tag = var.project_tag
  state_machine_arn = var.sfn_arn
  job_type = var.job_type

}

resource "aws_cloudwatch_event_rule" "cluster_schedule_rule" {
  name = "${local.resource_name_prefix}-jobs-coordinator-scheduled-event"
  schedule_expression = var.schedule_expression
  is_enabled = var.enable_schedule

  tags = {
    Name = "${local.resource_name_prefix}-jobs-coordinator-scheduled-event"
    Project = var.project_tag
    Environment = var.environment
    Type = var.job_type
  }
}

resource "aws_cloudwatch_event_target" "lambda_cloudwatch_event_target" {
  target_id = "${local.resource_name_prefix}-lambda-sfn-adapter"
  arn = module.lambda-sfn-adapter.lambda_arn
  rule = aws_cloudwatch_event_rule.cluster_schedule_rule.name
}

resource "aws_lambda_permission" "lambda_permission" {
  statement_id = "AllowExecutionFromCloudWatch"
  action = "lambda:InvokeFunction"
  function_name = module.lambda-sfn-adapter.lambda_name
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.cluster_schedule_rule.arn
}
