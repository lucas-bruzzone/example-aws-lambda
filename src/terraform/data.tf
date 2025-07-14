# Data source para VPC do state remoto
data "terraform_remote_state" "network" {
  backend = "s3"
  config = {
    bucket = "example-aws-terraform-terraform-state"
    key    = "example-aws-network/terraform.tfstate"
    region = "us-east-1"
  }
}