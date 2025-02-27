terraform {
  backend "s3" {
    bucket = "pds-infra"
    key    = "dev/pds_tf_infra.tfstate"
    region = "us-west-2"
  }
}
