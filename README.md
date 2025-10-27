# Carsale Payment Pipeline 
## Features

- Metadata-driven ingestion (YAML configs)
- SCD Type 2 with CDC
- Data quality checks
- CloudWatch logging(PENDING)
- Parallel execution
- CI/CD via GitHub Actions
- MWAA orchestration(PENDING)
- Uses existing VPC


## Project Structure
```
├── .github/workflows/     # CI/CD pipelines
├── terraform/             # Infrastructure
├── databricks/            # Processing
├── airflow/               # Orchestration
├── config/                # YAML configs
├── scripts/               # Deployment scripts
└── tests/                 # Unit tests
```

## Monitoring

- CloudWatch: `/databricks/payment-pipeline/`
- MWAA UI: Check terraform output for URL

