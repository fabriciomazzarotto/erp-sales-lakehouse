# Infraestrutura AWS — ERP Sales Lakehouse (Terraform)

> 🚧 **Status: código pronto, não aplicado.** Este projeto roda 100% local
> (`RUN_MODE=local`, ver `.env.example`). O Terraform abaixo define a
> infraestrutura AWS necessária para migrar (`RUN_MODE=aws`), validado com
> `terraform validate` (sintaxe e consistência de referências), mas **nunca
> foi rodado `terraform apply` contra uma conta AWS real**.

## O que este código provisiona

| Arquivo | Recurso |
|---|---|
| `s3.tf` | 4 buckets (bronze/silver/gold/diamond) + bucket de resultados do Athena — versionamento, criptografia SSE-S3, bloqueio de acesso público, lifecycle de custo (Bronze → IA após 90 dias; resultados do Athena expiram em 7 dias) |
| `iam.tf` | Role de execução do pipeline (least privilege: só os 4 buckets do Lakehouse, nunca `s3:*`) + role do Glue Crawler (leitura apenas) |
| `glue.tf` | Database no Glue Data Catalog + 1 crawler por camada, usando `delta_target` (nativo para Delta Lake — um crawler `s3_target` genérico tentaria interpretar o `_delta_log/` como dado e quebraria) |
| `athena.tf` | Workgroup dedicado, com corte de custo em 5 GB escaneados por query |

## Por que Terraform (e não clicar no console)

Reprodutibilidade e revisão em PR — a infraestrutura vira código versionado
igual ao resto do pipeline, em vez de um estado só documentado em texto que
ninguém consegue recriar com confiança depois. Também é o padrão de mercado
para este tipo de projeto de portfólio.

## Decisão de least privilege — paralelo com o SQL Server

O mesmo princípio usado no login `erp_extractor` do SQL Server (ver
`sql/04_create_pipeline_login.sql`: SELECT-only, nunca uma conta admin) foi
aplicado aqui: a role de execução do pipeline só tem `s3:GetObject` /
`s3:PutObject` / `s3:DeleteObject` / `s3:ListBucket` restritos às 4 buckets
do Lakehouse — nunca `"Resource": "*"`. A role do Glue Crawler é ainda mais
restrita (só leitura). Ver comentário no topo de `iam.tf` sobre o trade-off
de usar uma única role para as 4 camadas (vs. uma role por camada, que
exigiria um cluster/job Databricks por estágio).

## Quando for aplicar de verdade (checklist)

Este código **não deve ser aplicado** sem passar por isso primeiro:

1. Configurar credenciais AWS (`aws configure` ou variáveis de ambiente) — não commitadas em lugar nenhum do repo.
2. Copiar `terraform.tfvars.example` para `terraform.tfvars` (gitignored) e preencher os valores reais.
3. Decidir e configurar um backend remoto (S3 + DynamoDB lock) em `providers.tf` — o bloco comentado já está lá. Rodar com estado local só é aceitável para teste solo.
4. `terraform init && terraform plan` — **ler o plano inteiro** antes de aplicar.
5. `terraform apply`.
6. Copiar os `outputs` (nomes reais dos buckets, ARNs de role, nome do Glue database) para o `.env` do projeto e trocar `RUN_MODE=local` para `RUN_MODE=aws`.
7. Rodar os crawlers do Glue (`aws glue start-crawler --name ...`) depois da primeira carga de dados em cada bucket, para popular o Data Catalog.
8. Validar via Athena antes de conectar o Power BI (ver `docs/architecture.md`, seção de segurança/governança).
9. Agendar o pipeline (Databricks Jobs — os 4 notebooks em sequência, cron diário) e a atualização do dataset no Power BI Service (scheduled refresh via conector Athena, sem gateway) — ver `powerbi/README.md`, seção "Atualização automática", para o passo a passo completo e o porquê de cada peça.

## Comandos usados para validar este código (sem custo, sem credenciais AWS)

```bash
terraform fmt -recursive       # formatação
terraform init -backend=false  # baixa os providers, sem exigir backend/credenciais
terraform validate             # sintaxe + consistência de referências
```

`terraform plan`/`apply` **não** foram rodados — exigem credenciais AWS reais
e criariam recursos cobrados, o que está fora do escopo desta etapa do
projeto (portfólio local, sem custo de nuvem).
