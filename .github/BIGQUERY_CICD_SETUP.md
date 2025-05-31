# BigQuery CI/CD Setup Guide

This guide explains how to configure GitHub Actions for running BigQuery integration tests automatically.

## 🔧 Required GitHub Configuration

Before the CI/CD can run, you need to add secrets and variables to your GitHub repository:

### Navigate to Repository Settings
1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**

### Secrets (Sensitive Data)
Click **New repository secret** for each of the following:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `GCP_SA_KEY` | Google Cloud Service Account JSON key | `{"type": "service_account", "project_id": "your-project"...}` |
| `GCP_PROJECT_ID` | Google Cloud Project ID | `my-bigquery-project` |

### Variables (Non-Sensitive Configuration)
Click on the **Variables** tab, then **New repository variable** for each:

| Variable Name | Description | Example |
|---------------|-------------|---------|
| `BIGQUERY_DATABASE` | BigQuery dataset name for tests (optional, defaults to 'sqltesting') | `sqltesting` |

### 🔐 Setting up Google Cloud Service Account

Create a dedicated service account for GitHub Actions with minimal permissions:

#### 1. Create Service Account
```bash
# Set your project ID
export PROJECT_ID="your-project-id"

# Create service account
gcloud iam service-accounts create github-actions-bigquery \
    --display-name="GitHub Actions BigQuery" \
    --description="Service account for BigQuery integration tests" \
    --project=$PROJECT_ID
```

#### 2. Grant Required Permissions
```bash
# BigQuery Data Editor (to create/manage datasets and tables)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:github-actions-bigquery@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# BigQuery Job User (to run queries)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:github-actions-bigquery@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# BigQuery User (to access BigQuery resources)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:github-actions-bigquery@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.user"
```

#### 3. Create and Download Key
```bash
# Create service account key
gcloud iam service-accounts keys create github-actions-key.json \
    --iam-account=github-actions-bigquery@$PROJECT_ID.iam.gserviceaccount.com \
    --project=$PROJECT_ID

# Display the key content for copying to GitHub secrets
cat github-actions-key.json
```

#### 4. Alternative: Custom IAM Policy
For more granular control, create a custom policy:

```json
{
    "title": "GitHub Actions BigQuery Policy",
    "description": "Minimal permissions for BigQuery integration tests",
    "stage": "GA",
    "includedPermissions": [
        "bigquery.datasets.create",
        "bigquery.datasets.get",
        "bigquery.datasets.update",
        "bigquery.datasets.delete",
        "bigquery.tables.create",
        "bigquery.tables.get",
        "bigquery.tables.list",
        "bigquery.tables.update",
        "bigquery.tables.delete",
        "bigquery.tables.getData",
        "bigquery.jobs.create",
        "bigquery.jobs.get",
        "bigquery.jobs.list"
    ]
}
```

## 🏷️ Test Organization with Pytest Markers

Tests are organized using pytest markers for precise execution control:

### Marker Strategy
- **Unit tests**: No markers (default)
- **Integration tests**: `@pytest.mark.integration`
- **BigQuery-specific**: `@pytest.mark.bigquery`
- **Combined**: `@pytest.mark.integration` + `@pytest.mark.bigquery`

### Workflow Separation
- **`tests.yaml`**: Runs `pytest -m "not integration"` (unit tests only)
- **`bigquery-integration.yml`**: Runs `pytest -m "integration and bigquery"` (BigQuery integration only)

This ensures:
✅ Unit tests run on every commit (free)
✅ Integration tests run only for specific adapters
✅ No cross-contamination between test types
✅ Clear cost attribution per adapter

## 🏃‍♂️ Workflow Triggers

The CI/CD workflow runs automatically on:

### Pull Requests
- **Triggers**: Any PR to `master` or `main` branch
- **File changes**: Only when BigQuery-related files are modified:
  - `src/sql_testing_library/_adapters/bigquery.py`
  - `src/sql_testing_library/_core.py`
  - `src/sql_testing_library/_mock_table.py`
  - `tests/test_bigquery*.py`
  - `tests/integration/test_bigquery_integration.py`
  - Workflow file itself

### Push to Main/Master
- **Triggers**: Direct pushes to `master` or `main` branch
- **Runs**: Full test suite including real BigQuery tests

### Manual Trigger
- **Access**: Repository → Actions → "BigQuery Integration Tests" → "Run workflow"
- **Use case**: Testing without making commits

## 📊 Test Stages

### 1. Unit Tests (Always Free)
- Runs mocked BigQuery adapter tests
- No GCP resources used
- Fast execution (~2-3 minutes)

### 2. Mock Integration Tests (Always Free)
- Tests adapter logic with mocked BigQuery responses
- Validates SQL generation and parsing
- No GCP costs

### 3. Real Integration Tests (GCP Costs)
- Connects to real Google BigQuery
- Executes actual queries
- **Cost**: ~$0.01-0.10 per test run (depending on data processed)
- Includes automatic cleanup

## 💰 Cost Management

### Current Strategy (Automatic)
- **Every PR**: Real BigQuery tests run (~$0.05-0.10 per PR)
- **Every merge**: Real BigQuery tests run (~$0.05-0.10 per merge)
- **Estimated monthly cost**: $5-30 (depending on PR frequency)

### BigQuery Pricing Factors
- **Query processing**: $5 per TB processed
- **Storage**: $0.02 per GB per month
- **Streaming inserts**: $0.01 per 200MB
- **Test dataset**: Usually processes <1MB per test run

### Future Strategy (Manual - Post Release)
When ready to optimize costs, update the workflow condition:

```yaml
# Change this condition in .github/workflows/bigquery-integration.yml
if: |
  (github.event_name == 'push' && (github.ref == 'refs/heads/master' || github.ref == 'refs/heads/main')) ||
  (github.event_name == 'workflow_dispatch')
  # Remove: (github.event_name == 'pull_request')
```

This will:
- ✅ Run real tests on main branch pushes
- ✅ Allow manual triggers
- ❌ Skip real tests on PRs (only unit/mock tests)
- 💰 Reduce costs by ~80%

## 🔍 Monitoring and Debugging

### GitHub Actions Logs
- View detailed logs in: Repository → Actions → Workflow run
- Each step shows detailed output
- Failed tests include full error messages

### Test Results Summary
- Automatic summary appears in PR comments
- Shows which test stages passed/failed
- Includes cost estimation

### Common Issues

#### GCP Credentials Error
```
❌ GCP credentials not configured in GitHub secrets
```
**Solution**: Add `GCP_SA_KEY` and `GCP_PROJECT_ID` to repository secrets

#### Invalid Service Account Key
```
❌ GCP_SA_KEY: Set but invalid JSON format
```
**Solution**: Ensure the service account key is valid JSON and properly formatted

#### Permission Denied
```
❌ Access denied. Check service account permissions
```
**Solution**: Review IAM roles and ensure all required BigQuery permissions

#### Dataset Not Found
```
❌ Dataset 'project.dataset' not found
```
**Solution**: The workflow will automatically create the dataset, or check permissions

#### Query Quota Exceeded
```
❌ Query failed: Quota exceeded
```
**Solution**: Check BigQuery quotas and limits in GCP Console

## 🛡️ Security Best Practices

### ✅ Current Implementation
- Service account keys stored as GitHub secrets (encrypted)
- Minimal IAM permissions (BigQuery only)
- Automatic cleanup of test resources
- No credentials in logs or code
- Temporary credentials file cleanup

### 🔒 Additional Security (Optional)
- Use Workload Identity Federation instead of service account keys
- Restrict service account access by IP/time
- Enable Google Cloud Audit Logs
- Use Google Cloud Security Command Center

## 📈 BigQuery-Specific Features

### Dataset Management
- **Auto-creation**: Workflow creates test dataset if it doesn't exist
- **Location**: US region (configurable)
- **Cleanup**: Tables with 'test' or 'mock' in name are automatically removed

### Query Optimization
- **Dry runs**: Validate queries without processing data
- **Slot usage**: Monitor and limit concurrent query slots
- **Partitioning**: Use partitioned tables for large test datasets

### Cost Optimization
- **Query caching**: BigQuery automatically caches results
- **Approximate aggregation**: Use APPROX functions for large datasets
- **Sampling**: Use TABLESAMPLE for testing with large tables

## 🧪 Validation Script

Use the validation script to test your setup locally:

```bash
# Set up environment variables
export GCP_SA_KEY="$(cat path/to/service-account-key.json)"
export GCP_PROJECT_ID="your-project-id"
export BIGQUERY_DATABASE="sqltesting"  # optional

# Run validation
python scripts/validate-bigquery-setup.py
```

The script checks:
- ✅ Environment variables
- ✅ GCP connectivity
- ✅ BigQuery permissions
- ✅ Dataset access (creates if needed)
- ✅ Query execution
- ✅ Service account permissions

## 📈 Scaling for Multiple Adapters

When running multiple cloud adapters, consider:

### Parallel Execution
```yaml
strategy:
  matrix:
    adapter: [athena, bigquery, redshift, snowflake]
    include:
      - adapter: bigquery
        cloud: gcp
        secrets_prefix: GCP
```

### Cost Monitoring
- **BigQuery**: Use GCP Billing alerts
- **Cross-cloud**: Set up cost monitoring per adapter
- **Usage tracking**: Monitor test frequency and data processed

### Resource Isolation
- Separate service accounts per adapter
- Different projects/datasets for different test types
- Environment-specific configurations

## 🚀 Advanced Configuration

### Custom Query Engine Settings
```python
# In test configuration
client = bigquery.Client(
    project=project_id,
    default_query_job_config=bigquery.QueryJobConfig(
        dry_run=False,
        use_query_cache=True,
        maximum_bytes_billed=1000000,  # 1MB limit
        job_timeout=30,  # 30 second timeout
    )
)
```

### Large Dataset Testing
```python
# Use table sampling for performance tests
query = """
SELECT * FROM `project.dataset.large_table`
TABLESAMPLE SYSTEM (0.1 PERCENT)
LIMIT 1000
"""
```

### Performance Monitoring
```python
# Monitor query performance
job = client.query(query)
print(f"Bytes processed: {job.total_bytes_processed}")
print(f"Slot time: {job.slot_millis}")
print(f"Creation time: {job.created}")
```
