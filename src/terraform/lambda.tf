module "lambda_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 4.7"

  function_name = "${var.project_name}-properties-${var.environment}"
  source_path   = "../code"
  layers        = [module.lambda_layer.lambda_layer_arn]
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11" # Mudança aqui - python3.13 pode ter problemas com algumas libs
  timeout       = 30
  memory_size   = 512 # Aumentar memória para processamento de PDF

  # Usar seu role IAM existente
  create_role = false
  lambda_role = aws_iam_role.lambda.arn

  environment_variables = {
    PROPERTIES_TABLE = data.terraform_remote_state.dynamoDB.outputs.table_name
    ENVIRONMENT      = var.environment
  }

  depends_on = [module.lambda_layer]

  tags = {
    Name = "${var.project_name}-lambda"
  }
}

module "lambda_layer" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 4.7"

  create_function = false
  create_layer    = true

  layer_name          = "${var.project_name}-python-layer"
  description         = "Python dependencies for ${var.project_name}"
  compatible_runtimes = ["python3.11"]

  source_path = [
    {
      path             = "../lambda-layer"
      pip_requirements = true # Mudança aqui
      pip_tmp_dir      = "/tmp/lambda-layer-build"
      prefix_in_zip    = "python"
    }
  ]

  tags = {
    Name = "${var.project_name}-layer"
  }
}