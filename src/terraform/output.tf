output "lambda_function_name" {
  description = "Nome da função Lambda"
  value       = lambda_function.function_name
}

output "lambda_function_arn" {
  description = "ARN da função Lambda"
  value       = lambda_function.arn
}

output "lambda_invoke_arn" {
  description = "ARN de invocação da Lambda"
  value       = lambda_function.invoke_arn
}

output "lambda_role_arn" {
  description = "ARN da role da Lambda"
  value       = aws_iam_role.lambda.arn
}

output "lambda_security_group_id" {
  description = "ID do Security Group da Lambda"
  value       = aws_security_group.lambda.id
}