#!/usr/bin/env python3
"""
Setup script for Snowflake key-pair authentication.

This script helps generate RSA key pairs and configure them for Snowflake authentication,
which is ideal for CI/CD environments where MFA is not feasible.
"""

import os
import sys
import subprocess
import base64
from pathlib import Path


def generate_key_pair(key_path: str, passphrase: str = "") -> tuple[str, str]:
    """Generate RSA key pair for Snowflake authentication."""
    private_key_path = key_path
    public_key_path = f"{key_path}.pub"

    # Generate private key
    cmd = ["openssl", "genrsa", "-out", private_key_path]
    if passphrase:
        cmd.extend(["-aes256", "-passout", f"pass:{passphrase}"])
    cmd.append("2048")

    print(f"🔑 Generating private key: {private_key_path}")
    subprocess.run(cmd, check=True)

    # Generate public key
    cmd = ["openssl", "rsa", "-in", private_key_path, "-pubout", "-out", public_key_path]
    if passphrase:
        cmd.extend(["-passin", f"pass:{passphrase}"])

    print(f"🔑 Generating public key: {public_key_path}")
    subprocess.run(cmd, check=True)

    # Set appropriate permissions
    os.chmod(private_key_path, 0o600)

    return private_key_path, public_key_path


def get_public_key_fingerprint(public_key_path: str) -> str:
    """Get the fingerprint of the public key for Snowflake."""
    # Read the public key
    with open(public_key_path, 'r') as f:
        public_key = f.read()

    # Extract the key content (remove header/footer)
    key_lines = public_key.strip().split('\n')
    key_content = ''.join(line for line in key_lines if not line.startswith('-----'))

    # Decode base64
    key_bytes = base64.b64decode(key_content)

    # Calculate SHA256 fingerprint
    import hashlib
    fingerprint = hashlib.sha256(key_bytes).digest()

    # Format as Snowflake expects (base64 without padding)
    fingerprint_b64 = base64.b64encode(fingerprint).decode('utf-8').rstrip('=')

    return f"SHA256:{fingerprint_b64}"


def create_snowflake_setup_sql(user: str, public_key_path: str) -> str:
    """Generate SQL to configure the user in Snowflake."""
    # Read public key
    with open(public_key_path, 'r') as f:
        public_key = f.read()

    # Extract just the key content
    key_lines = public_key.strip().split('\n')
    key_content = ' '.join(line for line in key_lines if not line.startswith('-----'))

    sql = f"""
-- Run this SQL in Snowflake to configure key-pair authentication for user {user}

-- Set the public key for the user
ALTER USER {user} SET RSA_PUBLIC_KEY='{key_content}';

-- Verify the key was set
DESC USER {user};

-- Optional: Create a service user if needed (requires ACCOUNTADMIN role)
-- CREATE USER IF NOT EXISTS {user}_service
--   TYPE = SERVICE
--   RSA_PUBLIC_KEY = '{key_content}'
--   DEFAULT_ROLE = <your_role>
--   DEFAULT_WAREHOUSE = <your_warehouse>;

-- Grant necessary permissions to the service user
-- GRANT ROLE <your_role> TO USER {user}_service;
"""

    return sql


def create_github_actions_example(user: str, account: str) -> str:
    """Create example GitHub Actions configuration."""
    return f"""
# Example GitHub Actions workflow configuration

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
          SNOWFLAKE_ACCOUNT: {account}
          SNOWFLAKE_USER: {user}
          SNOWFLAKE_PRIVATE_KEY: ${{{{ secrets.SNOWFLAKE_PRIVATE_KEY }}}}
          # Optional: if private key is encrypted
          # SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: ${{{{ secrets.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE }}}}
        run: |
          pytest tests/

# To add the private key to GitHub secrets:
# 1. Go to Settings > Secrets and variables > Actions
# 2. Create a new secret named SNOWFLAKE_PRIVATE_KEY
# 3. Copy the entire private key file content (including headers)
# 4. If using passphrase, also add SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
"""


def create_pytest_ini_example() -> str:
    """Create example pytest.ini configuration."""
    return """
# Example pytest.ini configuration for key-pair authentication

[sql_testing.snowflake]
account = YOUR_ACCOUNT
user = YOUR_SERVICE_USER
# Remove password line - authentication will use private key
database = YOUR_DATABASE
schema = YOUR_SCHEMA
warehouse = YOUR_WAREHOUSE
role = YOUR_ROLE
# Optional: specify private key path (defaults to env var SNOWFLAKE_PRIVATE_KEY)
# private_key_path = /path/to/private_key.pem
"""


def main():
    """Main function to set up Snowflake key-pair authentication."""
    print("🚀 Snowflake Key-Pair Authentication Setup")
    print("=" * 50)
    print("\nThis script will help you set up key-pair authentication for Snowflake,")
    print("which is ideal for CI/CD environments where MFA is not feasible.\n")

    # Get user inputs
    user = input("Enter Snowflake username: ").strip()
    account = input("Enter Snowflake account (e.g., abc123.us-east-1): ").strip()

    key_dir = input("Enter directory for keys (default: ~/.snowflake): ").strip() or "~/.snowflake"
    key_dir = os.path.expanduser(key_dir)
    os.makedirs(key_dir, exist_ok=True)

    key_name = input("Enter key name (default: rsa_key): ").strip() or "rsa_key"
    key_path = os.path.join(key_dir, key_name)

    use_passphrase = input("Encrypt private key with passphrase? (y/N): ").strip().lower() == 'y'
    passphrase = ""
    if use_passphrase:
        import getpass
        passphrase = getpass.getpass("Enter passphrase: ")
        confirm = getpass.getpass("Confirm passphrase: ")
        if passphrase != confirm:
            print("❌ Passphrases don't match!")
            return 1

    # Generate keys
    try:
        private_key_path, public_key_path = generate_key_pair(key_path, passphrase)
        print(f"✅ Keys generated successfully!")
        print(f"   Private key: {private_key_path}")
        print(f"   Public key: {public_key_path}")
    except Exception as e:
        print(f"❌ Failed to generate keys: {e}")
        return 1

    # Get fingerprint
    try:
        fingerprint = get_public_key_fingerprint(public_key_path)
        print(f"\n📍 Public key fingerprint: {fingerprint}")
    except Exception as e:
        print(f"⚠️  Could not calculate fingerprint: {e}")

    # Generate SQL
    sql = create_snowflake_setup_sql(user, public_key_path)
    sql_file = f"{key_path}_setup.sql"
    with open(sql_file, 'w') as f:
        f.write(sql)
    print(f"\n📄 SQL setup script saved to: {sql_file}")

    # Generate GitHub Actions example
    github_example = create_github_actions_example(user, account)
    github_file = f"{key_path}_github_actions.yml"
    with open(github_file, 'w') as f:
        f.write(github_example)
    print(f"📄 GitHub Actions example saved to: {github_file}")

    # Generate pytest.ini example
    pytest_example = create_pytest_ini_example()
    pytest_file = f"{key_path}_pytest.ini"
    with open(pytest_file, 'w') as f:
        f.write(pytest_example)
    print(f"📄 pytest.ini example saved to: {pytest_file}")

    # Instructions
    print("\n" + "=" * 50)
    print("✅ Setup complete! Next steps:\n")
    print("1. Run the SQL script in Snowflake as ACCOUNTADMIN:")
    print(f"   snowsql -f {sql_file}")
    print("\n2. For GitHub Actions:")
    print("   - Copy your private key content to GitHub secrets")
    print("   - Name the secret: SNOWFLAKE_PRIVATE_KEY")
    if use_passphrase:
        print("   - Also add SNOWFLAKE_PRIVATE_KEY_PASSPHRASE secret")
    print("\n3. For local testing, set environment variable:")
    print(f"   export SNOWFLAKE_PRIVATE_KEY=$(cat {private_key_path})")
    if use_passphrase:
        print(f"   export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='your_passphrase'")
    print("\n4. Update your pytest.ini to remove password field")
    print("\n⚠️  Security notes:")
    print("   - Keep your private key secure!")
    print("   - Never commit private keys to version control")
    print("   - Use GitHub secrets or environment variables")
    print("   - Consider using separate service accounts for CI/CD")

    return 0


if __name__ == "__main__":
    sys.exit(main())
