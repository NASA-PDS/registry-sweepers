variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-west-2"
}

variable "config_file" {
  description = "Path for the environment-specific YAML task definition file"
  type        = string
}

variable "create_service" {
  description = "Flag to control where the ECS service should be created or not"
  type        = bool
  default     = true
}
variable "project" {
  description = "Tag value for project. Abbreviated project name."
  type        = string
}

variable "cicd" {
  description = "Tag value for CICD deployment method"
  type        = string
}
