output "task_definition_arns" {
  description = "Map of node name to ECS task definition ARN"
  value       = { for k, v in aws_ecs_task_definition.registry_sweepers : k => v.arn }
}

output "log_group_names" {
  description = "Map of node name to CloudWatch log group name"
  value       = { for k, v in aws_cloudwatch_log_group.registry_sweepers : k => v.name }
}

output "sweeper_task_role" {
  description = "Sweeper task role needed to update the OpenSearch data access policy"
  value       = aws_iam_role.task_role.arn
}
