output "lambda_arn" {
  value = aws_lambda_function.check_sfn_status_fn_lambda.arn
}
