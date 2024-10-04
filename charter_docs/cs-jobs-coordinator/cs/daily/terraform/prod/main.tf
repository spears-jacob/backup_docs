provider "aws" {
  version = "~> 3.13.0"
  region = var.region
}

terraform {
  backend "s3" {
    encrypt = true
    bucket = "charter-telemetry-pilot-tf-state"
    dynamodb_table = "terraform-state-lock-dynamo"
    key = "cs-jobs-coordinator-prod/terraform.tfstate"
    region = "us-east-1"
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
  "Comment": "Execution of all monthly jobs",
  "StartAt": "Parallel",
  "States": {
    "Parallel": {
      "Type": "Parallel",
      "ResultPath": null,
      "Next": "Get Tasks Status",
      "Branches": [
        {
          "StartAt": "cs-call-care-derived",
          "States": {
            "cs-call-care-derived": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-call-care-derived-execute-sfn",
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
                  "Next": "cs-call-care-derived-error-handler"
                }
              ],
              "Next": "cs-call-derived-parallel"
            },
            "cs-call-care-derived-error-handler": {
              "Type": "Pass",
              "End": true
            },
            "cs-call-derived-parallel": {
              "Type": "Parallel",
              "ResultPath": null,
              "End": true,
              "Branches": [
                {
                  "StartAt": "prod-monthly-final",
                  "States": {
                    "prod-monthly-final": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::states:startExecution.sync",
                      "Parameters": {
                        "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                        "Input": {
                          "LambdaArn": "${local.lambda_prefix}-prod-monthly-final-execute-sfn",
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
                          "Next": "prod-monthly-final-error-handler"
                        }
                      ],
                      "End": true
                    },
                    "prod-monthly-final-error-handler": {
                      "Type": "Pass",
                      "End": true
                    }
                  }
                },
                {
                  "StartAt": "with-prior-visits-agg",
                  "States": {
                    "with-prior-visits-agg": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::states:startExecution.sync",
                      "Parameters": {
                        "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                        "Input": {
                          "LambdaArn": "${local.lambda_prefix}-calls-prior-visits-agg-execute-sfn",
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
                          "Next": "w-prior-visits-agg-error-handler"
                        }
                      ],
                      "End": true
                    },
                    "w-prior-visits-agg-error-handler": {
                      "Type": "Pass",
                      "End": true
                    }
                  }
                }
              ]
            }
          }
        },
        {
          "StartAt": "cs-call-care-data-agg",
          "States": {
            "cs-call-care-data-agg": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-call-care-data-agg-execute-sfn",
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
                  "Next": "call-care-data-agg-error-handler"
                }
              ],
              "End": true
            },
            "call-care-data-agg-error-handler": {
              "Type": "Pass",
              "End": true
            }
          }
        },
        {
          "StartAt": "cs-pageview-daily",
          "States": {
            "cs-pageview-daily": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-pageview-daily-execute-sfn",
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
                  "Next": "cs-pageview-daily-error-handler"
                }
              ],
              "End": true
            },
            "cs-pageview-daily-error-handler": {
              "Type": "Pass",
              "End": true
            }
          }
        },
        {
          "StartAt": "pageview-click-aggregate",
          "States": {
            "pageview-click-aggregate": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-pageview-click-aggregate-execute-sfn",
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
                  "Next": "pageview-click-aggregate-error-handler"
                }
              ],
              "End": true
            },
            "pageview-click-aggregate-error-handler": {
              "Type": "Pass",
              "End": true
            }
          }
        },
        {
          "StartAt": "cid-cmp-aggregate",
          "States": {
            "cid-cmp-aggregate": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-cid-cmp-aggregate-execute-sfn",
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
                  "Next": "cid-cmp-aggregate-error-handler"
                }
              ],
              "End": true
            },
            "cid-cmp-aggregate-error-handler": {
              "Type": "Pass",
              "End": true
            }
          }
        },
        {
          "StartAt": "cs-jobs-msa",
          "States": {
            "cs-jobs-msa": {
              "Type": "Task",
              "Resource": "arn:aws:states:::states:startExecution.sync",
              "Parameters": {
                "StateMachineArn": "arn:aws:states:${var.region}:${var.aws_account_id}:stateMachine:${local.resource_name_prefix}-job-executor-sfn",
                "Input": {
                  "LambdaArn": "${local.lambda_prefix}-cs-jobs-msa-execute-sfn",
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
                  "Next": "cs-jobs-msa-error-handler"
                }
              ],
              "End": true
            },
            "cs-jobs-msa-error-handler": {
              "Type": "Pass",
              "End": true
            }
          }
        }
      ]
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
