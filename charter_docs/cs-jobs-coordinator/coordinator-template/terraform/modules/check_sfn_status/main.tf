locals {
  check_sfn_status_lambda_file = "coordinator-template/lambda/pkg/check_sfn_status_fn.zip"
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
}

resource "aws_iam_role" "check_sfn_status_fn_role" {
  name = "${local.resource_name_prefix}-check-sfn-status"
  path = "/"
  tags = {
    Name = "check_sfn_status_fn_role"
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
                   "Service": "lambda.amazonaws.com"
                },
                "Effect": "Allow",
                "Sid": ""
            }
        ]
}
EOF
}

resource "aws_iam_policy" "check_sfn_status_fn_policy" {
  name = "${local.resource_name_prefix}-check-sfn-status-fn-policy"
  policy = <<EOF
{
     "Version":"2012-10-17",
     "Statement":[
        {
           "Effect":"Allow",
           "Action":"states:*",
           "Resource":"*"
        },
        {
           "Effect":"Allow",
           "Action":[
              "logs:CreateLogGroup",
              "logs:CreateLogStream",
              "logs:PutLogEvents"
           ],
           "Resource":"*"
        }
     ]
}
EOF
}

resource "aws_iam_policy_attachment" "check_sfn_status_fn_policy_attachment" {
  name = "${local.resource_name_prefix}-check-sfn-status-fn-policy-attachment"
  roles = [
    aws_iam_role.check_sfn_status_fn_role.name]
  policy_arn = aws_iam_policy.check_sfn_status_fn_policy.arn
}

resource "aws_lambda_function" "check_sfn_status_fn_lambda" {
  filename = local.check_sfn_status_lambda_file
  function_name = "${local.resource_name_prefix}-check-sfn-status-fn"
  role = aws_iam_role.check_sfn_status_fn_role.arn
  handler = "check_sfn_status_fn.lambda_handler"
  source_code_hash = filebase64sha256(local.check_sfn_status_lambda_file)
  runtime = "python3.7"
  timeout = 30
  memory_size = 256
}
