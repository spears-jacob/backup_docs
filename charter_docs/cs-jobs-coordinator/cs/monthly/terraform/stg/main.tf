provider "aws" {
  version = "~> 3.13.0"
  region = var.region
}

terraform {
  backend "s3" {
    encrypt = true
    bucket = "charter-telemetry-pilot-tf-state-dev"
    dynamodb_table = "terraform-state-lock-dynamo"
    key = "cs-monthly-coordinator-stg/terraform.tfstate"
    region = "us-west-2"
  }
}

locals {
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
  lambda_prefix = "arn:aws:lambda:${var.region}:${var.aws_account_id}:function:${var.project_tag}-${var.environment}"
}

module "jobs-coordinator" {
  source = "../../../../coordinator-template/terraform/modules/coordinator_template"
  project_tag = var.project_tag
  environment = var.environment
  job_type = var.job_type
  region = var.region
  aws_account_id = var.aws_account_id
  enable_schedule = var.enable_schedule
  schedule_expression = var.schedule_expression
  definition = <<EOF
{
  "Comment": "Execution of monthly  cs jobs",
  "StartAt": "d-disposition-groupings",
  "States": {
    "d-disposition-groupings": {
      "Type": "Task",
      "Resource": "arn:aws:states:::states:startExecution.sync",
      "Parameters": {
        "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
        "Input": {
          "LambdaArn": "${local.lambda_prefix}-d-disposition-groupings-execute-sfn",
          "RunTime.$": "$$.Execution.Input.RUN_TIME",
          "SkipJobs.$": "$$.Execution.Input.SKIP_JOBS",
          "RunOnly.$": "$$.Execution.Input.RUN_ONLY",
          "CurrentJob.$": "$$.State.Name"
        }
      },
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "d-disposition-groupings-error-handler"
        }
      ],
      "Next": "Get Tasks Status"
    },
    "d-disposition-groupings-error-handler": {
      "Type": "Pass",
      "Next": "Get Tasks Status"
    },
    "Get Tasks Status": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "${local.resource_name_prefix}-check-tasks-status-fn",
        "Payload": {
          "SFN_EXECUTION_ARN.$": "$$.Execution.Id"
        }
      },
      "Next": "Check Status"
    },
    "Check Status": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.code",
          "NumericEquals": 0,
          "Next": "Flow Succeeded"
        }
      ],
      "Default": "Flow Failed"
    },
    "Flow Succeeded": {
      "Type": "Succeed"
    },
    "Flow Failed": {
      "Type": "Fail"
    }
  }
}
EOF
}
