variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment"
  type        = string
  default     = "devops"
}

variable "project_name" {
  description = "Project name"
  type        = string
}