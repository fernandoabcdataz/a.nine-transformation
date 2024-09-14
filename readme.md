# Transformation dbt Project

This dbt project handles data transformations for multiple clients using BigQuery.

## Project Structure

```
.
├── transformation/
│   ├── models/
│   ├── analyses/
│   ├── macros/
│   ├── tests/
│   ├── seeds/
│   ├── snapshots/
│   ├── dbt_project.yml
│   └── profiles.yml
├── Dockerfile
└── README.md
```

## Setup

1. Ensure you have dbt installed: `pip install dbt-bigquery`
2. Set up Google Cloud credentials
3. Update `transformation/profiles.yml` with your BigQuery connection details

## Configuration

- `transformation/dbt_project.yml`: Main project configuration
- `transformation/profiles.yml`: Connection profiles for different environments

## Usage

### Local Development

Run dbt commands:

```
cd transformation
dbt run --profiles-dir . --target dev --vars '{"client": "client_name", "project": "gcp_project"}'
```

### Docker Deployment

1. Build the Docker image:
   ```
   docker build --platform linux/amd64 -t gcr.io/[PROJECT-ID]/dbt-image:latest .
   ```

2. Push to Google Container Registry:
   ```
   docker push gcr.io/[PROJECT-ID]/dbt-image:latest
   ```

3. Deploy to Cloud Run (managed by Terraform)

## Terraform Deployment

Use the provided Terraform script to deploy:

```
terraform apply -var="client_name=clientA" -var="gcp_project=your-project" -var="region=us-central1"
```

## Customization

- Modify models in the `transformation/models/` directory
- Add tests in the `transformation/tests/` directory
- Create macros in the `transformation/macros/` directory

## Scheduling

The project is set up to run hourly via Cloud Scheduler.

## Support

For issues or questions, please contact [Your Contact Information].