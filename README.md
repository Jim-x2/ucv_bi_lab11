# ucv_bi_lab11
# Laboratorio 11 - Infraestructura Big Data con Terraform y Floci

## Descripción

Implementación de una infraestructura Big Data local mediante
Infrastructure as Code (IaC), utilizando Terraform y Floci
para emular servicios AWS.

## Tecnologías

- Terraform
- Docker Desktop
- Floci
- AWS CLI
- Git y GitHub

## Arquitectura

### Amazon S3 Data Lake

Bucket: ucv-data-platform

Dominios:
- sales
- customers
- products
- marketing

Capas:
- Bronze
- Silver
- Gold

### AWS Glue Data Catalog

Bases de datos:
- sales_domain
- customers_domain
- products_domain
- marketing_domain

## Ejecución

1. Iniciar Floci:

   docker compose up -d

2. Inicializar Terraform:

   cd terraform
   terraform init

3. Validar:

   terraform fmt
   terraform validate

4. Revisar el plan:

   terraform plan

5. Crear infraestructura:

   terraform apply

## Limpieza

terraform destroy
