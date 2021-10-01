terraform {
  backend "s3" {
    bucket = "gdc-tfstate"
    key    = "s3-gdc-explore"
    region = "us-west-2"
  }
}