terraform {
  backend "s3" {
    bucket = "pds-prod-infra"
    key    = "prod/registry_sweeper_ecs.tfstate"
    region = "us-west-2"
  }
}
