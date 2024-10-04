locals {
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
}

resource "aws_sfn_state_machine" "jobs_coordinator_sfn" {
  name = "${local.resource_name_prefix}-jobs-coordinator-sfn"
  role_arn = aws_iam_role.jobs_coordinator_role.arn
  definition = var.definition
}

module "check_sfn_status" {
  source = "../check_sfn_status"
  environment = var.environment
  project_tag = var.project_tag
  job_type = var.job_type
}

module "check_tasks_status" {
  source = "../check_tasks_status"
  environment = var.environment
  project_tag = var.project_tag
  job_type = var.job_type
}

module "lambda_executor" {
  source = "../lambda_executor"
  environment = var.environment
  project_tag = var.project_tag
  job_type = var.job_type
}

module "job_executor" {
  source = "../job_executor"
  environment = var.environment
  project_tag = var.project_tag
  region = var.region
  job_type = var.job_type
  check_sfn_lambda_arn = module.check_sfn_status.lambda_arn
  lambda_executor_lambda_arn = module.lambda_executor.lambda_arn
}

module "coordinator_multiple_executor" {
  source = "../multidate_coordinator_executor"
  environment = var.environment
  project_tag = var.project_tag
  aws_account_id = var.aws_account_id
  region = var.region
  job_type = var.job_type
  jobs_coordinator_arn = aws_sfn_state_machine.jobs_coordinator_sfn.id
}

module "scheduled_run" {
  source = "../scheduled_run"
  environment = var.environment
  project_tag = var.project_tag
  enable_schedule = var.enable_schedule
  job_type = var.job_type
  schedule_expression = var.schedule_expression
  sfn_arn = aws_sfn_state_machine.jobs_coordinator_sfn.id
}

# IAM resources
resource "aws_iam_role" "jobs_coordinator_role" {
  name = "${local.resource_name_prefix}-jobs-coordinator"
  path = "/"
  tags = {
    Name = "jobs_coordinator_role"
    Project = var.project_tag
    Environment = var.environment
    Type = var.job_type
  }
  assume_role_policy = <<EOF
{
      "Version": "2012-10-17",
      "Statement": [
          {
              "Action": "sts:AssumeRole",
              "Principal": {
                 "Service": "states.${var.region}.amazonaws.com"
              },
              "Effect": "Allow",
              "Sid": ""
          }
      ]
}
EOF
}

resource "aws_iam_policy" "jobs_coordinator_policy" {
  name = "${local.resource_name_prefix}-jobs-coordinator-policy"
  policy = <<EOF
{
          "Version": "2012-10-17",
          "Statement": [
              {
                  "Effect": "Allow",
                  "Action": [
                      "states:StartExecution"
                  ],
                  "Resource": "*"
              },
              {
                  "Effect": "Allow",
                  "Action": [
                      "states:DescribeExecution",
                      "states:StopExecution"
                  ],
                  "Resource": "*"
              },
              {
                  "Effect": "Allow",
                  "Action": [
                      "events:PutTargets",
                      "events:PutRule",
                      "events:DescribeRule"
                  ],
                  "Resource": [
                      "arn:aws:events:${var.region}:${var.aws_account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
                  ]
              },
              {
                  "Effect": "Allow",
                  "Action": [
                      "lambda:InvokeFunction"
                  ],
                  "Resource": "*"
              }

          ]
}
EOF
}

resource "aws_iam_policy_attachment" "jobs_coordinator_policy_attachment" {
  name = "${local.resource_name_prefix}-jobs-coordinator-policy-attachment"
  roles = [
    aws_iam_role.jobs_coordinator_role.name]
  policy_arn = aws_iam_policy.jobs_coordinator_policy.arn
}

