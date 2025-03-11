module "ecs" {
  source         = "git@github.com:NASA-PDS/pds-tf-modules.git//terraform/modules/ecs"
  config_file    = var.config_file
  create_service = var.create_service
  required_tags = {
    project = var.project
    cicd    = var.cicd
  }
}
