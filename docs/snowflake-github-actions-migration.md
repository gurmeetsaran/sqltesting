---
layout: default
title: Snowflake GitHub Actions - Key-Pair Setup
nav_order: 10
description: "Migrate Snowflake tests to GitHub Actions with key-pair authentication. Step-by-step guide for CI/CD integration."
---

# Migrating GitHub Actions from Password to Key-Pair Authentication
{: .no_toc }

Migrate Snowflake integration tests to GitHub Actions using key-pair authentication.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---

This guide helps you migrate your GitHub Actions workflows from password-based authentication to key-pair authentication for Snowflake, which is required when MFA is enabled.

## Why Migrate?

- **MFA Enforcement**: Snowflake accounts with MFA enabled cannot use password authentication in CI/CD
- **Security**: Key-pair authentication is more secure than storing passwords
- **Future-Proof**: Snowflake is phasing out password authentication for programmatic access by November 2025

## Migration Steps

### 1. Generate Key Pair

Run the setup script on your local machine:

```bash
python scripts/setup-snowflake-keypair.py
```

This will:
- Generate an RSA private/public key pair
- Save them to `~/.snowflake/` directory
- Create SQL statements for Snowflake configuration

### 2. Configure Snowflake

Execute the generated SQL in Snowflake as ACCOUNTADMIN or with appropriate privileges:

```sql
-- Set the public key for your CI/CD user
ALTER USER github_actions_user SET RSA_PUBLIC_KEY='<your_public_key_content>';

-- Verify the key was set
DESC USER github_actions_user;
```

### 3. Update GitHub Secrets

1. Go to your repository on GitHub
2. Navigate to **Settings** > **Secrets and variables** > **Actions**
3. Delete the old `SNOWFLAKE_PASSWORD` secret (if exists)
4. Create a new secret:
   - **Name**: `SNOWFLAKE_PRIVATE_KEY`
   - **Value**: Copy the entire content of your private key file (including headers)

Example private key format:
```
-----BEGIN RSA PRIVATE KEY-----
MIIEpgIBAAKCAQEA0i365dQErWgl4zo7+YdabqFK17B8uz81Z+vTTYJBs3HwZrkc
... (key content) ...
-----END RSA PRIVATE KEY-----
```

### 4. Update Your Workflow

The Snowflake integration workflow has been updated to use key-pair authentication. No changes needed if you're using the standard workflow.

If you have custom workflows, update them to:

**Before (Password Authentication):**
```yaml
env:
  SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

**After (Key-Pair Authentication):**
```yaml
env:
  SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
```

### 5. Test the Migration

1. Push a small change to trigger the workflow
2. Check the workflow logs for successful Snowflake connection
3. Verify tests are passing

## Troubleshooting

### "Failed to load private key"
- Ensure you copied the entire private key content including headers
- Check that the key is in PEM format (starts with `-----BEGIN RSA PRIVATE KEY-----`)
- Verify no extra whitespace or line breaks were added

### "Authentication failed"
- Verify the public key was correctly set in Snowflake
- Ensure the user account exists and is active
- Check that the account identifier is correct

### "Invalid private key"
- The key pair might not match - regenerate both keys
- Ensure you're using the correct private key for the configured public key

## Best Practices

1. **Rotate Keys Regularly**: Generate new key pairs every 90 days
2. **Use Separate Keys**: Don't reuse keys across different environments
3. **Secure Storage**: Never commit private keys to version control
4. **Minimal Permissions**: Grant only necessary permissions to CI/CD users

## Example GitHub Actions Configuration

```yaml
- name: Run Snowflake Tests
  env:
    SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
    SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
    SNOWFLAKE_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_PRIVATE_KEY }}
    SNOWFLAKE_DATABASE: TEST_DB
    SNOWFLAKE_WAREHOUSE: COMPUTE_WH
  run: |
    pytest tests/integration/ -m "snowflake"
```

## Support

If you encounter issues:
1. Check the [Snowflake MFA CI/CD documentation](snowflake-mfa-cicd.md)
2. Review the [setup script](../scripts/setup-snowflake-keypair.py)
3. Open an issue on GitHub with error details
