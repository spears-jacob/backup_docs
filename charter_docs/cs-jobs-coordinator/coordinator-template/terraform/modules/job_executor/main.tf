locals {
  resource_name_prefix = "${var.project_tag}-${var.environment}-${var.job_type}"
}

resource "aws_iam_role" "job_executor_role" {
  name = "${local.resource_name_prefix}-job-executor"
  path = "/"
  tags = {
    Name = "job-executor-role"
    Project = var.project_tag
    Environment = var.environment
    Type = var.job_type
  }
  assume_role_policy = <<EOF
{
     "Version":"2012-10-17",
     "Statement":[
        {
           "Action":"sts:AssumeRole",
           "Principal":{
              "Service":"states.${var.region}.amazonaws.com"
           },
           "Effect":"Allow",
           "Sid":""
        }
     ]
}
EOF
}

resource "aws_iam_policy" "job_executor_policy" {
  name = "${local.resource_name_prefix}-job-executor-policy"
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

resource "aws_iam_policy_attachment" "job_executor_policy_attachment" {
  name = "${local.resource_name_prefix}-job-executor-policy-attachment"
  roles = [
    aws_iam_role.job_executor_role.name]
  policy_arn = aws_iam_policy.job_executor_policy.arn
}

resource "aws_sfn_state_machine" "job_executor_sfn" {
  name = "${local.resource_name_prefix}-job-executor-sfn"
  role_arn = aws_iam_role.job_executor_role.arn
  definition = <<EOF
{
    "Comment":"Current job executor with skip feature",
    "StartAt":"JobExecution",
    "States":{
       "JobExecution":{
          "Type":"Task",
          "Resource":"${var.lambda_executor_lambda_arn}",
          "InputPath":"$",
          "OutputPath":"$",
          "ResultPath":"$.steps",
          "Next":"CheckJobStatusCode"
       },
       "CheckJobStatusCode":{
         "Type":"Choice",
         "Choices":[
           {
             "Variable":"$.steps.code",
             "NumericEquals":0,
             "Next":"CheckIfJobSkipped"
           }
         ],
          "Default":"JobFail"
       },
       "CheckIfJobSkipped":{
         "Type":"Choice",
         "Choices":[
           {
             "Variable":"$.steps.JobStatus",
             "StringEquals":"skip",
             "Next":"JobSkip"
           },
           {
             "Variable":"$.steps.JobStatus",
             "StringEquals":"execute",
             "Next":"WaitUntilComplete"
           }
         ],
         "Default":"WaitUntilComplete"
       },
       "WaitUntilComplete":{
          "Type":"Wait",
          "Seconds":100,
          "Next":"CheckSfnStatus"
       },
       "CheckSfnStatus":{
          "Type":"Task",
          "Resource":"${var.check_sfn_lambda_arn}",
          "Next":"EvaluateStatus",
          "InputPath":"$",
          "OutputPath":"$",
          "ResultPath":"$.steps.job_status"
       },
       "EvaluateStatus":{
          "Type":"Choice",
          "Choices":[
             {
                "Variable":"$.steps.job_status.code",
                "StringEquals":"Success",
                "Next":"JobSuccess"
             },
             {
                "Variable":"$.steps.job_status.code",
                "StringEquals":"Fail",
                "Next":"JobFail"
             }
          ],
          "Default":"WaitUntilComplete"
       },
       "JobSuccess":{
          "Type":"Succeed"
       },
       "JobFail":{
          "Type":"Fail"
       },
      "JobSkip":{
          "Type":"Succeed"
       }
    }
 }
EOF
}
