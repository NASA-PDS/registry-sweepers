# ==============================================================================
# main.tf
# ==============================================================================

# ------------------------------------------------------------------------------
# Data sources
# ------------------------------------------------------------------------------
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ------------------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------------------

variable "venue" {
  type        = string
  description = "Deployment venue (e.g. prod, uat)"
}

variable "aoss_endpoint" {
  type        = string
  description = "Registry AOSS endpoint url"
}

variable "aoss_collection_id" {
  type        = string
  description = "Registry AOSS collection ID"
}

variable "image_uri" {
  type        = string
  description = "registry-sweepers ECR image URI"
}

variable "permissions_boundary_policy_name" {
  type        = string
  description = "Name of the IAM policy to use as the permissions boundary for ECS roles"
}

variable "nodes" {
  type = map(object({
    cpu    = number
    memory = number
  }))
  description = "Map of node IDs to their ECS resource allocations"
}

# ------------------------------------------------------------------------------
# ECR Repository
# ------------------------------------------------------------------------------
resource "aws_ecr_repository" "registry_sweepers" {
  name                 = "pds-registry-sweepers"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  tags = {
    cicd    = "pds-github"
    project = "pds-registry-sweepers"
  }
}

# ------------------------------------------------------------------------------
# ECS Task Definitions (one per node)
# See iam.tf for the task_role and execution_role referenced below.
# ------------------------------------------------------------------------------
resource "aws_ecs_task_definition" "registry_sweepers" {
  for_each = var.nodes

  family                   = "pds-${each.key}-${var.venue}-registry-sweepers"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = each.value.cpu
  memory                   = each.value.memory
  task_role_arn            = aws_iam_role.task_role.arn
  execution_role_arn       = aws_iam_role.execution_role.arn

  runtime_platform {
    cpu_architecture        = "X86_64"
    operating_system_family = "LINUX"
  }

  container_definitions = jsonencode([
    {
      name      = "pds-${each.key}-${var.venue}-registry-sweepers-container"
      image     = var.image_uri
      cpu       = 0
      essential = true

      portMappings   = []
      mountPoints    = []
      volumesFrom    = []
      systemControls = []

      environment = [
        {
          name  = "PROV_ENDPOINT"
          value = var.aoss_endpoint
        },
        {
          name  = "MULTITENANCY_NODE_ID"
          value = each.key
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/pds-${each.key}-${var.venue}-registry-sweepers-task"
          "awslogs-create-group"  = "true"
          "awslogs-region"        = data.aws_region.current.name
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "date || exit 1"]
        interval    = 60
        timeout     = 5
        retries     = 3
        startPeriod = 300
      }
    }
  ])

  tags = {
    cicd    = "pds-github"
    project = "pds-${each.key}"
  }
}
