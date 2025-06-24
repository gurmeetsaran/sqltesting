#!/usr/bin/env python3
"""
Validation script for Redshift Serverless setup and connectivity.

This script helps validate that your local environment is properly configured
for Redshift integration testing.

Usage:
    python scripts/validate-redshift-setup.py

Requirements:
    - AWS credentials configured (via environment variables or AWS CLI)
    - boto3 library installed
    - psycopg2 library installed for direct connection testing
"""

import os
import sys
from typing import Dict, Optional


try:
    import boto3
    import psycopg2
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install required dependencies:")
    print("  pip install boto3 psycopg2-binary")
    sys.exit(1)


def check_environment_variables() -> Dict[str, Optional[str]]:
    """Check if required environment variables are set."""
    print("🔍 Checking environment variables...")

    env_vars = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-west-2"),
    }

    for var, value in env_vars.items():
        if value:
            if "SECRET" in var or "PASSWORD" in var:
                print(f"  ✅ {var}: {'*' * 8}")
            else:
                print(f"  ✅ {var}: {value}")
        else:
            if var in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]:
                print(f"  ❌ {var}: Not set (required)")
            else:
                print(f"  ⚠️  {var}: Not set (optional, will use default)")

    return env_vars


def check_aws_connectivity(region: str) -> bool:
    """Test AWS connectivity and permissions."""
    print(f"\n🔍 Testing AWS connectivity in region: {region}")

    try:
        # Test basic AWS connectivity
        sts_client = boto3.client("sts", region_name=region)
        identity = sts_client.get_caller_identity()
        print("  ✅ AWS Authentication successful")
        print(f"  ✅ Account ID: {identity.get('Account')}")
        print(f"  ✅ User ARN: {identity.get('Arn')}")

        # Test Redshift Serverless permissions
        redshift_client = boto3.client("redshift-serverless", region_name=region)

        # Try to list namespaces (this tests read permissions)
        try:
            namespaces = redshift_client.list_namespaces()
            print("  ✅ Redshift Serverless list permissions: OK")
            print(f"  ℹ️  Found {len(namespaces.get('namespaces', []))} existing namespaces")
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print("  ❌ Redshift Serverless permissions: Access denied")
                print("     Required permissions: redshift-serverless:ListNamespaces")
                return False
            else:
                print(f"  ⚠️  Redshift Serverless API test failed: {e}")

        return True

    except NoCredentialsError:
        print("  ❌ AWS credentials not found")
        print("     Please configure AWS credentials:")
        print("     - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
        print("     - Or run 'aws configure'")
        return False
    except Exception as e:
        print(f"  ❌ AWS connectivity failed: {e}")
        return False


def check_redshift_serverless_availability(region: str, namespace: str, workgroup: str) -> Optional[Dict]:
    """Check if Redshift Serverless resources exist and are available."""
    print("\n🔍 Checking Redshift Serverless resources...")

    # Import and use the management script
    import subprocess

    try:
        # Use the management script to check status
        result = subprocess.run([
            "python", "scripts/manage-redshift-cluster.py", "--region", region, "status"
        ], capture_output=True, text=True)

        if result.returncode == 0:
            print("  ✅ Redshift cluster status checked using management script")
            # Try to get endpoint if resources exist
            endpoint_result = subprocess.run([
                "python", "scripts/manage-redshift-cluster.py", "--region", region, "endpoint",
                "--admin-user", os.getenv("REDSHIFT_ADMIN_USER", "admin"),
                "--admin-password", os.getenv("REDSHIFT_ADMIN_PASSWORD", "dummy")
            ], capture_output=True, text=True)

            if endpoint_result.returncode == 0:
                # Parse pytest.ini if it was created
                try:
                    with open("pytest.ini", "r") as f:
                        content = f.read()
                        import re
                        host_match = re.search(r'host = (.+)', content)
                        port_match = re.search(r'port = (.+)', content)
                        database_match = re.search(r'database = (.+)', content)

                        if host_match and port_match and database_match:
                            return {
                                'host': host_match.group(1),
                                'port': int(port_match.group(1)),
                                'database': database_match.group(1)
                            }
                except FileNotFoundError:
                    pass
        else:
            print("  ⚠️  Redshift resources not available - will be created during testing")

        return None

    except Exception as e:
        print(f"  ❌ Error checking Redshift Serverless: {e}")
        return None


def test_redshift_connection(connection_info: Dict) -> bool:
    """Test direct connection to Redshift if endpoint is available."""
    print("\n🔍 Testing direct Redshift connection...")

    try:
        # Get credentials from environment variables
        admin_user = os.getenv("REDSHIFT_ADMIN_USER", "admin")
        admin_password = os.getenv("REDSHIFT_ADMIN_PASSWORD")

        if not admin_password:
            print("  ⚠️  REDSHIFT_ADMIN_PASSWORD not set, skipping connection test")
            print("     Set this environment variable to test direct connections")
            return False

        connection_string = (
            f"host={connection_info['host']} "
            f"port={connection_info['port']} "
            f"dbname={connection_info['database']} "
            f"user={admin_user} "
            f"password={admin_password}"
        )

        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

        # Test basic query
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print("  ✅ Connection successful")
        print(f"  ✅ Redshift version: {version[:50]}...")

        # Test create/drop table permissions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER,
                name VARCHAR(50)
            )
        """)
        cursor.execute("DROP TABLE IF EXISTS test_table")
        conn.commit()
        print("  ✅ Create/drop table permissions: OK")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"  ❌ Connection failed: {e}")
        print("     This might be normal if Redshift is not running")
        return False


def check_required_permissions() -> None:
    """Display required IAM permissions for Redshift integration testing."""
    print("\n📋 Required IAM Permissions for Redshift Integration Testing:")
    print("""
    The AWS user/role needs the following permissions:

    Redshift Serverless permissions:
    - redshift-serverless:CreateNamespace
    - redshift-serverless:DeleteNamespace
    - redshift-serverless:GetNamespace
    - redshift-serverless:ListNamespaces
    - redshift-serverless:CreateWorkgroup
    - redshift-serverless:DeleteWorkgroup
    - redshift-serverless:GetWorkgroup
    - redshift-serverless:ListWorkgroups
    - iam:CreateServiceLinkedRole (for Redshift service-linked role)
    - iam:GetRole
    - iam:ListRoles
    - ec2:DescribeAccountAttributes (for VPC and networking setup)
    - ec2:DescribeVpcs
    - ec2:DescribeSubnets
    - ec2:DescribeSecurityGroups
    - ec2:AuthorizeSecurityGroupIngress (for automatic security group configuration)

    Example IAM policy:
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "redshift-serverless:*",
                    "iam:CreateServiceLinkedRole",
                    "iam:GetRole",
                    "iam:ListRoles",
                    "ec2:DescribeAccountAttributes",
                    "ec2:DescribeVpcs",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:AuthorizeSecurityGroupIngress"
                ],
                "Resource": "*"
            }
        ]
    }

    Note: Redshift Serverless resources incur costs. The free trial provides
    $300 in credits for new users. Monitor your usage in the AWS console.
    """)


def main():
    """Main validation function."""
    print("🚀 Redshift Integration Testing Setup Validator")
    print("=" * 50)

    # Default configuration
    namespace = os.getenv("REDSHIFT_NAMESPACE", "sql-testing-ns")
    workgroup = os.getenv("REDSHIFT_WORKGROUP", "sql-testing-wg")

    # Check environment variables
    env_vars = check_environment_variables()
    region = env_vars["AWS_REGION"]

    if not env_vars["AWS_ACCESS_KEY_ID"] or not env_vars["AWS_SECRET_ACCESS_KEY"]:
        print("\n❌ Missing required AWS credentials")
        check_required_permissions()
        sys.exit(1)

    # Check AWS connectivity
    if not check_aws_connectivity(region):
        print("\n❌ AWS connectivity check failed")
        check_required_permissions()
        sys.exit(1)

    # Check Redshift Serverless resources
    connection_info = check_redshift_serverless_availability(region, namespace, workgroup)

    # Test connection if resources are available
    connection_success = False
    if connection_info:
        connection_success = test_redshift_connection(connection_info)

    # Summary
    print("\n📊 Validation Summary:")
    print("=" * 30)
    print("✅ Environment variables: OK")
    print("✅ AWS connectivity: OK")
    print(f"ℹ️  Redshift resources: {'Available' if connection_info else 'Will be created during testing'}")
    print(f"{'✅' if connection_success else 'ℹ️ '} Direct connection: {'OK' if connection_success else 'Not tested (resources not available)'}")

    print("\n🎯 Next Steps:")
    if connection_info and connection_success:
        print("  • Your environment is fully configured for Redshift testing")
        print("  • You can run integration tests immediately")
    else:
        print("  • Use the management script to create/manage Redshift resources:")
        print("    python scripts/manage-redshift-cluster.py create")
        print("  • Run integration tests manually:")
        print("    poetry run pytest tests/integration/test_redshift_integration.py -v")
        print("  • Clean up resources when done:")
        print("    python scripts/manage-redshift-cluster.py destroy")

    print("\n💰 Cost Information:")
    print("  • Redshift Serverless costs ~$3/hour minimum when active")
    print("  • Free trial provides $300 credit for new users")
    print("  • Resources are automatically cleaned up after tests")
    print("  • Monitor usage in AWS console to avoid unexpected charges")

    check_required_permissions()


if __name__ == "__main__":
    main()
