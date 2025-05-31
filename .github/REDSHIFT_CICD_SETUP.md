# Redshift CI/CD Integration Setup Guide

This guide explains how to set up automated Redshift integration testing using Amazon Redshift Serverless with GitHub Actions.

## Overview

The Redshift integration tests use **Amazon Redshift Serverless** to run real SQL tests against a live Redshift instance. This ensures your SQL Testing Library works correctly with Redshift's specific SQL dialect and behavior.

### Cost Information

- **Free Trial**: $300 credit for new AWS accounts (90-day expiration)
- **Pay-per-use**: ~$3/hour minimum when active
- **Auto-cleanup**: Resources are automatically destroyed after tests
- **Monitoring**: Always monitor usage in AWS console

## Prerequisites

### 1. AWS Account Setup

1. **AWS Account**: You need an AWS account with Redshift Serverless access
2. **IAM User**: Create an IAM user with Redshift Serverless permissions
3. **Programmatic Access**: Generate access keys for CI/CD

### 2. IAM Permissions

Create an IAM policy with the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "redshift-serverless:CreateNamespace",
                "redshift-serverless:DeleteNamespace",
                "redshift-serverless:GetNamespace",
                "redshift-serverless:ListNamespaces",
                "redshift-serverless:CreateWorkgroup",
                "redshift-serverless:DeleteWorkgroup",
                "redshift-serverless:GetWorkgroup",
                "redshift-serverless:ListWorkgroups",
                "iam:CreateServiceLinkedRole",
                "iam:GetRole",
                "iam:ListRoles",
                "ec2:DescribeAccountAttributes",
                "ec2:DescribeVpcs",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups",
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:RevokeSecurityGroupIngress",
                "ec2:DescribeInternetGateways",
                "ec2:DescribeAddresses",
                "ec2:DescribeAvailabilityZones"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateServiceLinkedRole"
            ],
            "Resource": "arn:aws:iam::*:role/aws-service-role/redshift-serverless.amazonaws.com/*",
            "Condition": {
                "StringEquals": {
                    "iam:AWSServiceName": "redshift-serverless.amazonaws.com"
                }
            }
        }
    ]
}
```

**Important Permissions Explained:**

- **`ec2:AuthorizeSecurityGroupIngress`**: Allows the script to automatically add security group rules during cluster creation to enable Redshift connectivity (port 5439 access)
- **`ec2:RevokeSecurityGroupIngress`**: ⚠️ **NEW REQUIREMENT** - Allows the script to automatically remove security group rules during cluster destruction to clean up open inbound traffic from 0.0.0.0/0 for enhanced security
- **`ec2:DescribeSecurityGroups`**: Required to identify which security groups are associated with the Redshift workgroup and to check existing rules

**Security Benefits:**
The enhanced `manage-redshift-cluster.py` script now automatically removes security group rules that allow traffic from all IP addresses (0.0.0.0/0) when destroying clusters. This prevents leaving behind insecure inbound rules after testing is complete.

### 3. Service Account Setup

1. **Create IAM User**:
   ```bash
   aws iam create-user --user-name redshift-ci-user
   ```

2. **Attach Policy**:
   ```bash
   aws iam attach-user-policy \
     --user-name redshift-ci-user \
     --policy-arn arn:aws:iam::ACCOUNT:policy/RedshiftServerlessCI
   ```

3. **Create Access Keys**:
   ```bash
   aws iam create-access-key --user-name redshift-ci-user
   ```

## GitHub Repository Configuration

### Required Secrets

Add these secrets to your GitHub repository (Settings → Secrets and variables → Actions):

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIAIOSFODNN7EXAMPLE` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |
| `REDSHIFT_ADMIN_PASSWORD` | Redshift admin password | `SecurePass123!` |
| `REDSHIFT_ADMIN_USER` | Redshift admin username (optional) | `admin` |

### Optional Variables

Add these variables for customization (Settings → Secrets and variables → Actions → Variables):

| Variable Name | Description | Default |
|---------------|-------------|---------|
| `AWS_REGION` | AWS region for Redshift | `us-east-1` |
| `REDSHIFT_NAMESPACE` | Redshift namespace name | `sql-testing-ns` |
| `REDSHIFT_WORKGROUP` | Redshift workgroup name | `sql-testing-wg` |

### Password Requirements

The `REDSHIFT_ADMIN_PASSWORD` must meet these criteria:
- 8-64 characters long
- At least one uppercase letter
- At least one lowercase letter
- At least one number
- Can contain special characters: `!@#$%^&*()_+-=[]{}|;:,.<>?`

**Examples of valid passwords:**
- `TestPass123!`
- `SecureRedshift2024#`
- `IntegrationTest789$`

## Workflow Configuration

### Automatic Triggers

The Redshift integration tests run automatically when:

1. **Pull Requests** that modify:
   - `src/sql_testing_library/_adapters/redshift.py`
   - `src/sql_testing_library/_core.py`
   - `src/sql_testing_library/_mock_table.py`
   - `tests/test_redshift*.py`
   - `tests/integration/test_redshift_integration.py`
   - `.github/workflows/redshift-integration.yml`

2. **Pushes to master** that modify the same files

3. **Manual triggers** via GitHub Actions UI

4. **Release workflow** calls (runs all tests regardless of file changes)

### Workflow Steps

The Redshift integration workflow:

1. **Setup Environment**:
   - Installs Python and dependencies
   - Configures AWS credentials

2. **Create Redshift Resources**:
   - Creates Redshift Serverless namespace
   - Creates Redshift Serverless workgroup
   - Waits for resources to be available

3. **Configure Testing**:
   - Retrieves Redshift endpoint information
   - Generates pytest configuration
   - Sets up database connection parameters

4. **Run Tests**:
   - Executes comprehensive integration tests
   - Tests various SQL operations and edge cases

5. **Cleanup**:
   - Removes auto-created security group rules (enhanced security)
   - Destroys all Redshift resources
   - Ensures no ongoing costs
   - Prevents security risks from leftover open inbound rules

## Local Development

### Setup Local Environment

1. **Install Dependencies**:
   ```bash
   poetry install --with dev,redshift
   ```

2. **Configure AWS Credentials**:
   ```bash
   export AWS_ACCESS_KEY_ID="your-access-key"
   export AWS_SECRET_ACCESS_KEY="your-secret-key"
   export AWS_REGION="us-east-1"
   ```

3. **Set Redshift Credentials** (for validation):
   ```bash
   export REDSHIFT_ADMIN_USER="admin"
   export REDSHIFT_ADMIN_PASSWORD="YourSecurePassword123!"
   ```

### Validate Setup

Run the validation script to check your configuration:

```bash
python scripts/validate-redshift-setup.py
```

This script will:
- ✅ Check AWS credentials and permissions
- ✅ Test Redshift Serverless API access
- ✅ Validate existing resources (if any)
- ✅ Test direct connection (if resources exist)
- ✅ Display cost information and setup guidance

### Enhanced Security Features

The `manage-redshift-cluster.py` script now includes enhanced security features:

**Automatic Security Group Management:**
```bash
# Create cluster (adds security rules for port 5439 access)
python scripts/manage-redshift-cluster.py create

# Destroy cluster (automatically removes open security rules)
python scripts/manage-redshift-cluster.py destroy

# Manual security group cleanup (if needed)
python scripts/manage-redshift-cluster.py cleanup-sg

# Skip security cleanup during destroy (if you want to preserve rules)
python scripts/manage-redshift-cluster.py destroy --skip-sg-cleanup
```

**Security Benefits:**
- Automatically removes inbound rules allowing traffic from 0.0.0.0/0
- Only removes auto-created rules (identified by description)
- Prevents leaving behind insecure access after testing
- Follows AWS security best practices

### Manual Testing

Create a local pytest.ini for manual testing:

```ini
[sql_testing]
adapter = redshift
host = your-redshift-endpoint.redshift-serverless.us-east-1.amazonaws.com
database = sqltesting_db
user = admin
password = YourSecurePassword123!
port = 5439
```

Run specific tests:
```bash
poetry run pytest tests/integration/test_redshift_integration.py -v
```

## Cost Management

### Free Tier Benefits

- **New AWS accounts**: $300 credit with 90-day expiration
- **Existing accounts**: Check if Redshift Serverless free trial is available

### Cost Optimization

1. **Automatic Cleanup**: Resources are destroyed after each test run
2. **Path Filtering**: Tests only run when relevant files change
3. **Timeout Protection**: Tests have 15-minute timeout to prevent runaway costs
4. **Manual Control**: Release tests are manually triggered

### Monitoring Costs

1. **AWS Cost Explorer**: Monitor Redshift Serverless usage
2. **CloudWatch**: Set up billing alerts
3. **AWS Budgets**: Create budget alerts for Redshift usage

**Typical Test Run Cost**: ~$0.50-$1.00 per run (depending on test duration)

## Troubleshooting

### Common Issues

**1. Permission Denied**
```
Error: AccessDenied: User is not authorized to perform redshift-serverless:CreateNamespace
```
**Solution**: Verify IAM permissions include all required Redshift Serverless actions

**1b. Security Group Permission Denied**
```
Warning: Could not remove rules from security group: UnauthorizedOperation:
You are not authorized to perform: ec2:RevokeSecurityGroupIngress
```
**Solution**: Ensure IAM permissions include `ec2:RevokeSecurityGroupIngress` for security group cleanup during cluster destruction. This is required for the enhanced security features that automatically remove open inbound rules.

**2. Resource Creation Timeout**
```
Error: Namespace creation timeout
```
**Solution**:
- Check AWS service health in your region
- Try a different AWS region
- Manual cleanup and retry

**3. Connection Refused**
```
Error: Connection refused to Redshift endpoint
```
**Solution**:
- Verify endpoint is publicly accessible
- Check security group settings
- Ensure workgroup is fully available

**4. Password Validation Error**
```
Error: Password does not meet requirements
```
**Solution**: Ensure password meets Redshift requirements:
- 8-64 characters
- Contains uppercase, lowercase, number
- Avoid special characters that might cause shell issues

### Manual Cleanup

If resources aren't cleaned up automatically:

```bash
# Delete workgroup
aws redshift-serverless delete-workgroup \
  --workgroup-name sql-testing-wg \
  --region us-east-1

# Delete namespace
aws redshift-serverless delete-namespace \
  --namespace-name sql-testing-ns \
  --region us-east-1
```

### Debug Information

Check GitHub Actions logs for:
- AWS API responses
- Redshift resource creation status
- Connection string generation
- Test execution details

## Security Considerations

### Credential Management

- ✅ **Never commit credentials** to source code
- ✅ **Use GitHub Secrets** for all sensitive information
- ✅ **Rotate access keys** regularly
- ✅ **Monitor usage** for unauthorized access

### Network Security

- ✅ **Public accessibility** required for GitHub Actions
- ✅ **Temporary resources** minimize exposure window
- ✅ **Automatic cleanup** prevents persistent security risks

### Best Practices

1. **Least Privilege**: IAM policy includes only required permissions
2. **Temporary Resources**: No persistent infrastructure
3. **Audit Logging**: CloudTrail logs all Redshift API calls
4. **Regular Review**: Periodically review access patterns

## Integration with Release Process

The Redshift integration tests are automatically included in the release workflow:

1. **Manual Release Trigger**: Runs comprehensive test suite
2. **All Adapters Tested**: Redshift, Athena, BigQuery, and unit tests
3. **Release Blocked**: If any integration tests fail
4. **Quality Assurance**: Ensures production readiness

This ensures that every release is tested against real Redshift infrastructure before being published to PyPI.

## Support and Maintenance

### Updating Tests

When adding new Redshift-specific features:

1. Add unit tests to `tests/test_redshift*.py`
2. Add integration tests to `tests/integration/test_redshift_integration.py`
3. Update this documentation if configuration changes

### AWS Service Updates

Monitor AWS announcements for:
- Redshift Serverless feature updates
- API changes
- Pricing modifications
- Regional availability changes

### Workflow Maintenance

Periodically review and update:
- GitHub Actions versions
- Python and dependency versions
- AWS CLI version
- Test timeout values

For questions or issues, refer to the project's main documentation or create an issue in the repository.
