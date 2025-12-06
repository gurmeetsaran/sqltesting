---
layout: default
title: Snowflake MFA & Key-Pair Auth for CI/CD
nav_order: 9
description: "Setup Snowflake key-pair authentication for CI/CD when MFA is enabled. Configure GitHub Actions with private key authentication."
---

# Snowflake MFA Authentication for CI/CD
{: .no_toc }

Configure key-pair authentication for Snowflake when MFA is enforced in CI/CD environments.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

When Snowflake requires Multi-Factor Authentication (MFA), you cannot use password-based authentication in CI/CD environments like GitHub Actions. This guide explains how to set up key-pair authentication as an alternative.

## Overview

Snowflake supports several authentication methods for programmatic access:
1. **Password authentication** - Not suitable when MFA is enforced
2. **External browser authentication** - Requires interactive login, not suitable for CI/CD
3. **Key-pair authentication** - Recommended for CI/CD environments
4. **OAuth** - For advanced use cases

## Setting Up Key-Pair Authentication

### 1. Generate Key Pair

Run the provided setup script:

```bash
python scripts/setup-snowflake-keypair.py
```

This will:
- Generate an RSA private/public key pair
- Create SQL statements to configure Snowflake
- Generate example configurations for GitHub Actions and pytest

### 2. Configure Snowflake

Execute the generated SQL as an ACCOUNTADMIN in Snowflake:

```sql
ALTER USER your_user SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';
```

### 3. Configure Your Environment

#### For GitHub Actions

Add the private key to GitHub Secrets:
1. Go to Settings > Secrets and variables > Actions
2. Create a new secret named `SNOWFLAKE_PRIVATE_KEY`
3. Copy the entire private key content (including headers)

Update your workflow:

```yaml
env:
  SNOWFLAKE_ACCOUNT: your-account
  SNOWFLAKE_USER: your-user
  SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
```

#### For Local Testing

Set environment variable:
```bash
export SNOWFLAKE_PRIVATE_KEY=$(cat ~/.snowflake/rsa_key)
```

Or update pytest.ini:
```ini
[sql_testing.snowflake]
account = your-account
user = your-user
private_key_path = /path/to/private_key.pem
# Remove password line
```

## Creating Service Users (Recommended)

For better security, create dedicated service users for CI/CD:

```sql
-- Create service user (no MFA required for SERVICE type)
CREATE USER IF NOT EXISTS ci_service_user
  TYPE = SERVICE
  RSA_PUBLIC_KEY = 'MIIBIjANBgkq...'
  DEFAULT_ROLE = your_role
  DEFAULT_WAREHOUSE = your_warehouse;

-- Grant necessary permissions
GRANT ROLE your_role TO USER ci_service_user;
```

Service users with `TYPE = SERVICE`:
- Cannot use password authentication
- Are exempt from MFA requirements
- Are designed for programmatic access

## Authentication Priority

The Snowflake adapter checks for authentication methods in this order:
1. Private key (via `private_key_path` or `SNOWFLAKE_PRIVATE_KEY` env var)
2. Authenticator parameter (e.g., `externalbrowser`, OAuth)
3. Password authentication
4. Error if no method is provided

## Troubleshooting

### "Multi-factor authentication is required"
- Solution: Switch to key-pair authentication following this guide

### "Private key not found"
- Check that the private key path is correct
- Ensure the SNOWFLAKE_PRIVATE_KEY environment variable is set correctly
- Verify file permissions (should be readable by the process)

### "Invalid private key"
- Ensure you're using the correct key format (PEM)
- Check that the public key was properly configured in Snowflake
- Verify the key pair matches (public key in Snowflake, private key in your app)

## Security Best Practices

1. **Never commit private keys** to version control
2. **Use encrypted private keys** with passphrases for additional security
3. **Rotate keys regularly** (every 90 days recommended)
4. **Use separate service accounts** for different environments
5. **Restrict service account permissions** to minimum required
6. **Enable network policies** to restrict access by IP address

## Example GitHub Actions Workflow

```yaml
name: Test with Snowflake
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -e ".[snowflake]"

      - name: Run tests
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
          SNOWFLAKE_DATABASE: TEST_DB
          SNOWFLAKE_WAREHOUSE: COMPUTE_WH
        run: |
          pytest tests/
```
