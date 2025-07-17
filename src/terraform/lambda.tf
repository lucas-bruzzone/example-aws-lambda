# ===================================
# LAMBDA FUNCTION WITH LAYER
# ===================================

module "lambda_function" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "~> 4.7"

  function_name = "${var.project_name}-properties-${var.environment}"
  source_path   = "../../code"
  layers        = [module.lambda_layer.lambda_layer_arn]
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.13"
  timeout       = 30     # Aumentado para geração de PDF
  memory_size   = 256    # Aumentado para ReportLab
  
  attach_policy = true
  policy        = aws_iam_policy.lambda_policy.arn

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
  compatible_runtimes = ["python3.13"]

  source_path = [
    {
      path             = "../../lambda-layer"
      pip_requirements = true
      prefix_in_zip    = "python"
    }
  ]

  store_on_s3 = false

  tags = {
    Name = "${var.project_name}-layer"
  }
}