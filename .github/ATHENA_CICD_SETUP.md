# Athena CI/CD Setup Guide

This guide explains how to configure GitHub Actions for running Athena integration tests automatically.

## 🔧 Required GitHub Configuration

Before the CI/CD can run, you need to add secrets and variables to your GitHub repository:

### Navigate to Repository Settings
1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**

### Secrets (Sensitive Data)
Click **New repository secret** for each of the following:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS Access Key ID for Athena access | `AKIAEXAMPLE123` |
| `AWS_SECRET_ACCESS_KEY` | AWS Secret Access Key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `AWS_ATHENA_OUTPUT_LOCATION` | S3 location for Athena query results | `s3://your-bucket/athena-results/` |

### Variables (Non-Sensitive Configuration)
Click on the **Variables** tab, then **New repository variable** for each:

| Variable Name | Description | Example |
|---------------|-------------|---------|
| `AWS_ATHENA_DATABASE` | Athena database name for tests | `test_db` |
| `AWS_REGION` | AWS region (optional, defaults to us-west-2) | `us-west-2` |

### 🔐 Setting up AWS IAM User

Create a dedicated IAM user for GitHub Actions with minimal permissions:

#### 1. Create IAM Policy
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:ListQueryExecutions",
                "athena:ListWorkGroups"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:List*",
                "s3:Get*"
            ],
            "Resource": [
                "arn:aws:s3:::your-athena-results-bucket",
                "arn:aws:s3:::your-athena-results-bucket/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        }
    ]
}
```

#### 2. Create IAM User
1. Create user: `github-actions-athena`
2. Attach the policy created above
3. Generate access keys
4. Add keys to GitHub secrets

## 🏷️ Test Organization with Pytest Markers

Tests are organized using pytest markers for precise execution control:

### Marker Strategy
- **Unit tests**: No markers (default)
- **Integration tests**: `@pytest.mark.integration`
- **Athena-specific**: `@pytest.mark.athena`
- **Combined**: `@pytest.mark.integration` + `@pytest.mark.athena`

### Workflow Separation
- **`tests.yaml`**: Runs `pytest -m "not integration"` (unit tests in `tests/` only)
- **`athena-integration.yml`**: Runs `pytest tests/integration/ -m "integration and athena"` (Athena integration only)

This ensures:
✅ Unit tests run on every commit (free)
✅ Integration tests run only for specific adapters
✅ No cross-contamination between test types
✅ Clear cost attribution per adapter

## 🏃‍♂️ Workflow Triggers

The CI/CD workflow runs automatically on:

### Pull Requests
- **Triggers**: Any PR to `master` or `main` branch
- **File changes**: Only when Athena-related files are modified:
  - `src/sql_testing_library/adapters/athena.py`
  - `src/sql_testing_library/core.py`
  - `src/sql_testing_library/mock_table.py`
  - `tests/test_athena*.py`
  - `tests/integration/test_athena_integration.py`
  - Workflow file itself

### Push to Main/Master
- **Triggers**: Direct pushes to `master` or `main` branch
- **Runs**: Full test suite including real Athena tests

### Manual Trigger
- **Access**: Repository → Actions → "Athena Integration Tests" → "Run workflow"
- **Use case**: Testing without making commits

## 📊 Test Stages

### 1. Unit Tests (Always Free)
- Runs mocked Athena adapter tests
- No AWS resources used
- Fast execution (~2-3 minutes)

### 2. Mock Integration Tests (Always Free)
- Tests adapter logic with mocked AWS responses
- Validates SQL generation and parsing
- No AWS costs

### 3. Real Integration Tests (AWS Costs)
- Connects to real AWS Athena
- Executes actual queries
- **Cost**: ~$0.01-0.05 per test run
- Includes automatic cleanup

## 💰 Cost Management

### Current Strategy (Automatic)
- **Every PR**: Real Athena tests run (~$0.05 per PR)
- **Every merge**: Real Athena tests run (~$0.05 per merge)
- **Estimated monthly cost**: $5-20 (depending on PR frequency)

### Future Strategy (Manual - Post Release)
When ready to optimize costs, update the workflow condition:

```yaml
# Change this condition in .github/workflows/athena-integration.yml
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

#### AWS Credentials Error
```
❌ AWS credentials not configured in GitHub secrets
```
**Solution**: Add all required secrets to repository settings

#### Connection Timeout
```
❌ Failed to connect to AWS Athena: timeout
```
**Solution**: Check AWS region and network connectivity

#### Permission Denied
```
❌ User: arn:aws:iam::xxx:user/github-actions is not authorized
```
**Solution**: Review IAM policy and ensure all required permissions

## 🛡️ Security Best Practices

### ✅ Current Implementation
- AWS credentials stored as GitHub secrets (encrypted)
- Minimal IAM permissions
- Automatic cleanup of test resources
- No credentials in logs or code

### 🔒 Additional Security (Optional)
- Use AWS IAM roles with OIDC instead of access keys
- Restrict S3 bucket access by IP/time
- Enable AWS CloudTrail for audit logging

## 📈 Scaling for Multiple Adapters

When adding other adapters (BigQuery, Redshift, Trino), the workflow can be extended:

```yaml
strategy:
  matrix:
    adapter: [athena, bigquery, redshift, trino]
    include:
      - adapter: athena
        secrets_suffix: ATHENA
      - adapter: bigquery
        secrets_suffix: BIGQUERY
```

This allows running all adapter tests in parallel while maintaining cost control.
