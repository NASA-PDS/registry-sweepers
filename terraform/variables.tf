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

variable "mwaa_execution_role_name" {
  type        = string
  description = "Name of the MWAA execution role that needs iam:PassRole to launch ECS tasks"
}

variable "nodes" {
  type = map(object({
    cpu    = number
    memory = number
  }))
  description = "Map of node IDs to their ECS resource allocations"
}
