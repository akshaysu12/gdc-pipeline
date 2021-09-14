provider "aws" {
  region = "us-west-2"
}

resource "aws_s3_bucket" "gdc-explore" {
  bucket = "gdc-explore"
  acl    = "private"

  tags = {
    Name        = "gdc-explore"
  }
}