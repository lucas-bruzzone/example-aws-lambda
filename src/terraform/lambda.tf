# Lambda Layer (para requirements)
resource "aws_lambda_layer_version" "requirements" {
  count = fileexists("../requirements/requirements.txt") ? 1 : 0

  filename            = data.archive_file.layer[0].output_path
  layer_name          = "${var.project_name}-requirements"
  compatible_runtimes = ["python3.13"]
  source_code_hash    = data.archive_file.layer[0].output_base64sha256
}

# Archive para layer (requirements)
data "archive_file" "layer" {
  count = fileexists("../requirements/requirements.txt") ? 1 : 0

  type        = "zip"
  source_dir  = "../requirements"
  output_path = "/tmp/${var.project_name}-layer.zip"
}

# Archive para código da Lambda
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "../code"
  output_path = "/tmp/${var.project_name}-lambda.zip"
}

# Lambda Function
resource "aws_lambda_function" "lambda_function" {
  filename      = data.archive_file.lambda.output_path
  function_name = "${var.project_name}-function"
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.13"
  timeout       = 30
  memory_size   = 128

  source_code_hash = data.archive_file.lambda.output_base64sha256

  vpc_config {
    subnet_ids         = data.terraform_remote_state.network.outputs.public_subnet_ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  layers = fileexists("../requirements/requirements.txt") ? [aws_lambda_layer_version.requirements[0].arn] : []

  environment {
    variables = {
      ENVIRONMENT = var.environment
    }
  }
}