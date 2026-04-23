venue              = "prod"
aoss_endpoint      = "https://..."
aoss_collection_id = "..."
image_uri          = "123456789012.dkr.ecr.us-west-2.amazonaws.com/pds-registry-sweepers:latest"

permissions_boundary_policy_name = "..."

# ECS resource allocations for each node
nodes = {
  "exampleNode" = { cpu = 1024, memory = 4096 }
}

#### N.B. The following must be incorporated appropriately into the new terraform - edunn 20260423
# variable "project" {
#   description = "Tag value for project. Abbreviated project name."
#   type        = string
# }
#
# variable "cicd" {
#   description = "Tag value for CICD deployment method"
#   type        = string
# }
