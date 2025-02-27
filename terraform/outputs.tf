output "ecs_clusters" {
  description = "ECS Cluster ID"
  value       = module.ecs.ecs_cluster_id
}

output "ecs_task_definitions" {
  description = "ECS Task Definitions ARNS"
  value       = module.ecs.task_definition_arn
}

output "ecs_services" {
  description = "ECS Service Names"
  value       = module.ecs.ecs_service_name
}
