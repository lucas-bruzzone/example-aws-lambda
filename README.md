Este repositório contém um template simples para provisionamento de recursos AWS utilizando Terraform.

## Como usar

1. Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/example-aws-terraform-template.git
    ```
2. Configure suas credenciais AWS.
3. Inicialize o Terraform:
    ```bash
    terraform init
    ```
4. Aplique a infraestrutura:
    ```bash
    terraform apply
    ```

## Estrutura

- `main.tf`: Definições principais dos recursos AWS.
- `variables.tf`: Variáveis utilizadas no projeto.
- `outputs.tf`: Saídas dos recursos provisionados.

## Requisitos

- [Terraform](https://www.terraform.io/downloads.html) instalado
- Conta AWS válida

## Licença

Este projeto está sob a licença MIT.
