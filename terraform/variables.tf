# ------------------------------------------------------------------------------
# Variables
# ------------------------------------------------------------------------------

variable "venue" {
  type        = string
  description = "Deployment venue (e.g. prod, uat)"
}

variable "managedby" {
  type        = string
  description = "Person or system doing the deployment."
}

variable "aws_region" {
  type        = string
  description = "AWS Region of the deployment."
}

variable "aoss_endpoint" {
  type        = string
  description = "Registry AOSS endpoint url"
}

variable "image_uri" {
  type        = string
  description = "registry-sweepers ECR image URI"
}

variable "mwaa_execution_role_name" {
  type        = string
  description = "Name of the MWAA execution role that needs iam:PassRole to launch ECS tasks"
  default     = ""
}

variable "nodes" {
  type = map(object({
    cpu             = number
    memory          = number
    additional_args = optional(string)
  }))
  description = "Map of node IDs to their ECS resource allocations. Optional 'ecs_task_cmd' overrides the container command."
}
