output "task_definition_arns" {
  description = "Map of node name to ECS task definition ARN"
  value       = { for k, v in aws_ecs_task_definition.registry_sweepers : k => v.arn }
}
