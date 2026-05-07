output "task_definition_arns" {
  description = "Map of node name to ECS task definition ARN"
  value       = { for k, v in aws_ecs_task_definition.registry_sweepers : k => v.arn }
}


output "sweeper_task_role" {
  description = "Sweeper task role needed to update the OpenSearch data access policy"
  value       = aws_iam_role.task_role.arn
}
