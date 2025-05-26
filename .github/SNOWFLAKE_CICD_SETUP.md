# Snowflake CI/CD Setup Guide

This guide explains how to configure GitHub Actions for running Snowflake integration tests automatically.

## 🔧 Required GitHub Configuration

Before the CI/CD can run, you need to add secrets and variables to your GitHub repository:

### Navigate to Repository Settings
1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**

### Secrets (Sensitive Data)
Click **New repository secret** for each of the following:

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `test_user` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `SecurePassword123` |

### Variables (Non-Sensitive Configuration)
Click on the **Variables** tab, then **New repository variable** for each:

| Variable Name | Description | Example      |
|---------------|-------------|--------------|
| `SNOWFLAKE_DATABASE` | Snowflake database name for tests | `TEST_DB`    |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse (required) | `COMPUTE_WH` |
| `SNOWFLAKE_ROLE` | Snowflake role (optional) | `DEVELOPER`  |
| `SNOWFLAKE_SCHEMA` | Snowflake schema (optional, defaults to PUBLIC) | `SQLTESTING` |

### 🔐 Setting up Snowflake User

Create a dedicated user for GitHub Actions with minimal permissions:

#### 1. Create Database and Schema
```sql
-- Create test database
CREATE DATABASE TEST_DB;

-- Create schema (optional, can use PUBLIC)
CREATE SCHEMA TEST_DB.SQLTESTING;

```

#### 2. Create User and Role
```sql
-- Create role for CI/CD
CREATE ROLE github_actions_role;

-- Grant permissions
GRANT USAGE ON DATABASE TEST_DB TO ROLE github_actions_role;
GRANT USAGE ON SCHEMA TEST_DB.SQLTESTING TO ROLE github_actions_role;
GRANT CREATE TABLE ON SCHEMA TEST_DB.SQLTESTING TO ROLE github_actions_role;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA TEST_DB.SQLTESTING TO ROLE github_actions_role;

-- Grant warehouse usage (CRITICAL: replace COMPUTE_WH with your actual warehouse name)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE github_actions_role;

-- Create user
CREATE USER github_actions_user
  PASSWORD = 'SecurePassword123'
  DEFAULT_ROLE = github_actions_role
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = TEST_DB.SQLTESTING;

-- Grant role to user
GRANT ROLE github_actions_role TO USER github_actions_user;
```

#### 3. Minimal Permissions Policy
The user needs permissions for:
- **Database**: `USAGE` on test database
- **Schema**: `USAGE` and `CREATE TABLE` on test schema
- **Tables**: `SELECT`, `INSERT`, `DELETE`, `DROP` on test tables
- **Warehouse**: `USAGE` on compute warehouse (CRITICAL for query execution)

#### 4. Troubleshooting Warehouse Issues

**Error: "Object does not exist, or operation cannot be performed"**

This usually means:
1. **Warehouse doesn't exist**: Check `SHOW WAREHOUSES;` to see available warehouses
2. **No permission**: Ensure `GRANT USAGE ON WAREHOUSE <name> TO ROLE <role>;` was executed
3. **Wrong warehouse name**: Verify the exact warehouse name (case-sensitive)

**Check warehouse access:**
```sql
-- List available warehouses
SHOW WAREHOUSES;

-- Check current role's warehouse permissions
SHOW GRANTS TO ROLE github_actions_role;

-- Test warehouse usage manually
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_WAREHOUSE();
```

## 🏷️ Test Organization with Pytest Markers

Tests are organized using pytest markers for precise execution control:

### Marker Strategy
- **Unit tests**: No markers (default)
- **Integration tests**: `@pytest.mark.integration`
- **Snowflake-specific**: `@pytest.mark.snowflake`
- **Combined**: `@pytest.mark.integration` + `@pytest.mark.snowflake`

### Workflow Separation
- **`tests.yaml`**: Runs `pytest -m "not integration"` (unit tests in `tests/` only)
- **`snowflake-integration.yml`**: Runs `pytest tests/integration/ -m "integration and snowflake"` (Snowflake integration only)

This ensures:
✅ Unit tests run on every commit (free)
✅ Integration tests run only for specific adapters
✅ No cross-contamination between test types
✅ Clear cost attribution per adapter

## 🏃‍♂️ Workflow Triggers

The CI/CD workflow runs automatically on:

### Pull Requests
- **Triggers**: Any PR to `master` or `main` branch
- **File changes**: Only when Snowflake-related files are modified:
  - `src/sql_testing_library/adapters/snowflake.py`
  - `src/sql_testing_library/core.py`
  - `src/sql_testing_library/mock_table.py`
  - `tests/test_snowflake*.py`
  - `tests/integration/test_snowflake_integration.py`
  - Workflow file itself

### Push to Main/Master
- **Triggers**: Direct pushes to `master` or `main` branch
- **Runs**: Full test suite including real Snowflake tests

### Manual Trigger
- **Access**: Repository → Actions → "Snowflake Integration Tests" → "Run workflow"
- **Use case**: Testing without making commits

## 📊 Test Stages

### 1. Unit Tests (Always Free)
- Runs mocked Snowflake adapter tests
- No Snowflake resources used
- Fast execution (~2-3 minutes)

### 2. Mock Integration Tests (Always Free)
- Tests adapter logic with mocked Snowflake responses
- Validates SQL generation and parsing
- No Snowflake costs

### 3. Real Integration Tests (Snowflake Costs)
- Connects to real Snowflake account
- Executes actual queries
- **Cost**: ~$0.01-0.10 per test run (depending on warehouse size)
- Includes automatic cleanup of temporary tables

## 💰 Cost Management

### Current Strategy (Automatic)
- **Every PR**: Real Snowflake tests run (~$0.05 per PR)
- **Every merge**: Real Snowflake tests run (~$0.05 per merge)
- **Estimated monthly cost**: $5-20 (depending on PR frequency and warehouse size)

### Cost Factors
- **Warehouse size**: X-Small warehouses cost less than larger ones
- **Query complexity**: Simple tests cost less than complex analytical queries
- **Data volume**: Tests use minimal mock data to reduce costs
- **Auto-suspend**: Warehouses auto-suspend after 1 minute of inactivity

### Future Strategy (Manual - Post Release)
When ready to optimize costs, update the workflow condition:

```yaml
# Change this condition in .github/workflows/snowflake-integration.yml
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

#### Snowflake Authentication Error
```
❌ Snowflake authentication failed: Incorrect username or password
```
**Solution**: Verify credentials in GitHub secrets match Snowflake user

#### Permission Denied
```
❌ SQL compilation error: Insufficient privileges to operate on table
```
**Solution**: Review user permissions and ensure all required grants

#### Connection Timeout
```
❌ Failed to connect to Snowflake: timeout
```
**Solution**: Check account identifier format and network connectivity

#### Warehouse Not Available
```
❌ Warehouse 'COMPUTE_WH' does not exist or not authorized
```
**Solution**: Verify warehouse name and user has USAGE permission

## 🛡️ Security Best Practices

### ✅ Current Implementation
- Snowflake credentials stored as GitHub secrets (encrypted)
- Minimal user permissions
- Automatic cleanup of test resources (temporary tables)
- No credentials in logs or code
- Dedicated test database/schema

### 🔒 Additional Security (Optional)
- Use Snowflake key-pair authentication instead of password
- Enable network policies to restrict access by IP
- Set up resource monitors to prevent unexpected costs
- Enable query tags for better cost tracking

## 📈 Scaling for Multiple Environments

When adding multiple Snowflake environments (dev, staging, prod), the workflow can be extended:

```yaml
strategy:
  matrix:
    environment: [dev, staging]
    include:
      - environment: dev
        account: ${{ secrets.SNOWFLAKE_DEV_ACCOUNT }}
        database: TEST_DB_DEV
      - environment: staging
        account: ${{ secrets.SNOWFLAKE_STAGING_ACCOUNT }}
        database: TEST_DB_STAGING
```

This allows running tests against different Snowflake environments while maintaining isolation.

## 🔧 Validation Script

To validate your Snowflake setup before running CI/CD:

```bash
# Run validation script
python scripts/validate-snowflake-setup.py
```

This script will:
- Test connection to Snowflake
- Verify user permissions
- Check database and schema access
- Validate warehouse usage
- Test temporary table creation and cleanup
