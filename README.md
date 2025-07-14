# example-aws-lambda

Repositório base para infraestrutura Terraform

## Configuração

O repositório já está configurado com:
- Workflow do GitHub Actions para deploy automático
- Secret `AWS_ROLE_ARN` configurado
- Estrutura base do Terraform

## Deploy

Deploy automático via GitHub Actions quando há push na branch main.

Deploy manual:
```bash
cd terraform
terraform init
terraform plan -var-file="tfvars/devops.tfvars"
terraform apply -var-file="tfvars/devops.tfvars"
```

## Estrutura

- `terraform/` - Arquivos Terraform
- `.github/workflows/` - GitHub Actions
- `tfvars/` - Arquivos de variáveis
