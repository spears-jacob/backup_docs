locals {
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
  mc_input_formatter_file = "coordinator-template/lambda/pkg/mc_input_formatter_fn.zip"
}

# IAM resources
resource "aws_iam_role" "multidate_coordinator_executor_role" {
  name = "${local.resource_name_prefix}-mc-executor"
  path = "/"
  tags = {
    Name = "mc-executor-role"
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

resource "aws_iam_policy" "multidate_coordinator_executor_policy" {
  name = "${local.resource_name_prefix}-multidate-coordinator-executor-policy"
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

resource "aws_iam_policy_attachment" "multidate_coordinator_executor_policy_attachment" {
  name = "${local.resource_name_prefix}-multidate-coordinator-executor-policy-attachment"
  roles = [
    aws_iam_role.multidate_coordinator_executor_role.name]
  policy_arn = aws_iam_policy.multidate_coordinator_executor_policy.arn
}

resource "aws_sfn_state_machine" "multidate_coordinator_executor_sfn" {
  name = "${local.resource_name_prefix}-multidate-coordinator-executor-sfn"
  role_arn = aws_iam_role.multidate_coordinator_executor_role.arn
  definition = <<EOF
{
  "Comment":"RUN_ONLY,RUN_TIME,SKIP_JOBS parameters are required!",
  "StartAt": "Input Formatting",
  "States": {
    "Input Formatting": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.mc_input_formatter.arn}",
      "Next": "Map"
    },
    "Map": {
      "Type": "Map",
      "InputPath": "$.params",
      "ItemsPath": "$",
      "Next": "Final State",
      "MaxConcurrency": 1,
      "Parameters": {
        "params.$": "$$.Map.Item.Value"
      },
      "Iterator": {
        "StartAt": "Pass Parameters",
        "States": {
          "Pass Parameters": {
            "ResultPath": "$",
            "Type": "Pass",
            "Next": "Execute Coordinators"
          },
          "Execute Coordinators": {
            "Type": "Task",
            "Resource": "arn:aws:states:::states:startExecution.sync",
            "Parameters": {
              "StateMachineArn": "${var.jobs_coordinator_arn}",
              "Input": {
                "RUN_TIME.$": "$.params.RUN_TIME",
                "SKIP_JOBS.$": "$.params.SKIP_JOBS",
                "RUN_ONLY.$": "$.params.RUN_ONLY"
              }
            },
            "Catch": [
              {
                "ErrorEquals": [
                  "States.TaskFailed"
                ],
                "Next": "Error Handler"
              }
            ],
            "End": true
          },
          "Error Handler": {
            "Type": "Pass",
            "End": true
          }
        }
      }
    },
    "Final State": {
      "Type": "Pass",
      "End": true
    }
  }
}
EOF
}

resource "aws_iam_role" "mc_input_formatter_role" {
  name = "${local.resource_name_prefix}-mc-inp-form-exec-fn"
  path = "/"
  tags = {
    Name = "mc-input-formatter-fn-role"
    Project = var.project_tag
    Environment = var.environment
  }
  assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "lambda.amazonaws.com"
        },
        "Effect": "Allow",
        "Sid": ""
      }
    ]
}
EOF
}

resource "aws_iam_policy" "mc_input_formatter_policy" {
  name = "${local.resource_name_prefix}-mc-input-formatter-fn-policy"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:${var.region}:${var.aws_account_id}:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:${var.region}:${var.aws_account_id}:log-group:/aws/lambda/${aws_lambda_function.mc_input_formatter.function_name}:*"
            ]
        }
    ]
}
EOF
}

resource "aws_iam_policy_attachment" "lambda_executor_policy_attachment" {
  name = "${local.resource_name_prefix}-lambda-executor-policy-attachment"
  roles = [
    aws_iam_role.mc_input_formatter_role.name]
  policy_arn = aws_iam_policy.mc_input_formatter_policy.arn
}

resource "aws_lambda_function" "mc_input_formatter" {
  filename = local.mc_input_formatter_file
  function_name = "${local.resource_name_prefix}-mc-input-formatter-fn"
  role = aws_iam_role.mc_input_formatter_role.arn
  handler = "mc_input_formatter_fn.lambda_handler"
  source_code_hash = filebase64sha256(local.mc_input_formatter_file)
  runtime = "python3.8"
  memory_size = "1024"
  timeout = "300"
}
