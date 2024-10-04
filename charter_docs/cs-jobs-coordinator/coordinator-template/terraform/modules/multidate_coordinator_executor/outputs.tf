output "sfn_arn" {
  value = aws_sfn_state_machine.multidate_coordinator_executor_sfn.id
}
