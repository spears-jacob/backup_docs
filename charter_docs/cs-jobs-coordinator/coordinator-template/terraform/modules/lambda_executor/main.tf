locals {
  lambda_executor_lambda_file = "coordinator-template/lambda/pkg/lambda_executor_fn.zip"
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
}
resource "aws_iam_role" "lambda_executor_role" {
  name = "${local.resource_name_prefix}-lambda-exec-fn"
  path = "/"
  tags = {
    Name = "lambda-exec-fn-role"
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

resource "aws_iam_policy" "lambda_executor_policy" {
  name = "${local.resource_name_prefix}-lambda-executor-lambda-fn-policy"
  policy = <<EOF
{
      "Version": "2012-10-17",
      "Statement": [
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

resource "aws_iam_policy_attachment" "lambda_executor_policy_attachment" {
  name = "${local.resource_name_prefix}-lambda-executor-policy-attachment"
  roles = [
    aws_iam_role.lambda_executor_role.name]
  policy_arn = aws_iam_policy.lambda_executor_policy.arn
}

resource "aws_lambda_function" "lambda_executor_lambda" {
  filename = local.lambda_executor_lambda_file
  function_name = "${local.resource_name_prefix}-lambda-executor-lambda"
  role = aws_iam_role.lambda_executor_role.arn
  handler = "lambda_executor_fn.lambda_handler"
  source_code_hash = filebase64sha256(local.lambda_executor_lambda_file)
  runtime = "python3.7"
  timeout = 10
}
