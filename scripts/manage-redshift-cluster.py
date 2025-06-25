#!/usr/bin/env python3
"""
Redshift Serverless cluster management script.

This script provides a unified interface for creating, managing, and destroying
Redshift Serverless resources for testing purposes.

Usage:
    python scripts/manage-redshift-cluster.py create --namespace test-ns --workgroup test-wg
    python scripts/manage-redshift-cluster.py status --namespace test-ns --workgroup test-wg
    python scripts/manage-redshift-cluster.py destroy --namespace test-ns --workgroup test-wg  # Always waits for deletion completion
    python scripts/manage-redshift-cluster.py endpoint --workgroup test-wg
    python scripts/manage-redshift-cluster.py configure-sg --workgroup test-wg
    python scripts/manage-redshift-cluster.py cleanup-sg --workgroup test-wg

Note: The 'destroy' command now automatically removes security group rules that
      allow traffic from all IP addresses (0.0.0.0/0) for enhanced security.
"""

import argparse
import os
import sys
import time
from typing import Dict, Optional


try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install required dependencies:")
    print("  pip install boto3")
    sys.exit(1)


class RedshiftClusterManager:
    """Manages Redshift Serverless clusters for testing."""

    def __init__(self, region: str = "us-west-2"):
        """Initialize the cluster manager."""
        self.region = region
        self.client = boto3.client("redshift-serverless", region_name=region)
        self.ec2_client = boto3.client("ec2", region_name=region)

    def create_namespace(
        self,
        namespace_name: str,
        admin_user: str = "admin",
        admin_password: str = None,
        database_name: str = "sqltesting_db"
    ) -> bool:
        """Create a Redshift Serverless namespace."""
        print(f"🚀 Creating namespace: {namespace_name}")

        if not admin_password:
            admin_password = os.getenv("REDSHIFT_ADMIN_PASSWORD")
            if not admin_password:
                print("❌ Admin password is required")
                print("Set REDSHIFT_ADMIN_PASSWORD environment variable or pass --admin-password")
                return False

        try:
            response = self.client.create_namespace(
                namespaceName=namespace_name,
                dbName=database_name,
                adminUsername=admin_user,
                adminUserPassword=admin_password
            )

            print(f"✅ Namespace creation initiated: {response['namespace']['namespaceName']}")
            print(f"ℹ️  Status: {response['namespace']['status']}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ConflictException':
                print(f"ℹ️  Namespace {namespace_name} already exists")
                return True
            else:
                print(f"❌ Failed to create namespace: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error creating namespace: {e}")
            return False

    def wait_for_namespace(self, namespace_name: str, timeout: int = 600) -> bool:
        """Wait for namespace to be available."""
        print(f"⏳ Waiting for namespace {namespace_name} to be available...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.client.get_namespace(namespaceName=namespace_name)
                status = response['namespace']['status']

                if status == 'AVAILABLE':
                    print(f"✅ Namespace {namespace_name} is available")
                    return True
                elif status in ['DELETING', 'MODIFYING']:
                    print(f"ℹ️  Namespace status: {status}, waiting...")
                    time.sleep(10)
                else:
                    print(f"ℹ️  Namespace status: {status}")
                    time.sleep(5)

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print(f"❌ Namespace {namespace_name} not found")
                    return False
                print(f"❌ Error checking namespace status: {e}")
                return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return False

        print(f"⏰ Timeout waiting for namespace {namespace_name}")
        return False

    def create_workgroup(
        self,
        workgroup_name: str,
        namespace_name: str,
        base_capacity: int = 8,
        publicly_accessible: bool = True
    ) -> bool:
        """Create a Redshift Serverless workgroup."""
        print(f"🚀 Creating workgroup: {workgroup_name}")

        try:
            response = self.client.create_workgroup(
                workgroupName=workgroup_name,
                namespaceName=namespace_name,
                baseCapacity=base_capacity,
                publiclyAccessible=publicly_accessible
            )

            print(f"✅ Workgroup creation initiated: {response['workgroup']['workgroupName']}")
            print(f"ℹ️  Status: {response['workgroup']['status']}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ConflictException':
                print(f"ℹ️  Workgroup {workgroup_name} already exists")
                return True
            else:
                print(f"❌ Failed to create workgroup: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error creating workgroup: {e}")
            return False

    def wait_for_workgroup(self, workgroup_name: str, timeout: int = 600) -> bool:
        """Wait for workgroup to be available."""
        print(f"⏳ Waiting for workgroup {workgroup_name} to be available...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.client.get_workgroup(workgroupName=workgroup_name)
                status = response['workgroup']['status']

                if status == 'AVAILABLE':
                    print(f"✅ Workgroup {workgroup_name} is available")
                    return True
                elif status in ['CREATING', 'MODIFYING']:
                    print(f"ℹ️  Workgroup status: {status}, waiting...")
                    time.sleep(10)
                else:
                    print(f"ℹ️  Workgroup status: {status}")
                    time.sleep(5)

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print(f"❌ Workgroup {workgroup_name} not found")
                    return False
                print(f"❌ Error checking workgroup status: {e}")
                return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return False

        print(f"⏰ Timeout waiting for workgroup {workgroup_name}")
        return False

    def get_endpoint_info(self, workgroup_name: str, quiet: bool = False) -> Optional[Dict[str, str]]:
        """Get endpoint information for a workgroup."""
        try:
            response = self.client.get_workgroup(workgroupName=workgroup_name)
            endpoint = response['workgroup'].get('endpoint')

            if not endpoint:
                if not quiet:
                    print(f"ℹ️  Workgroup {workgroup_name} has no endpoint yet")
                return None

            info = {
                'host': endpoint['address'],
                'port': str(endpoint['port']),
                'workgroup': workgroup_name
            }

            if not quiet:
                print(f"🔗 Endpoint: {info['host']}:{info['port']}")
            return info

        except ClientError as e:
            print(f"❌ Error getting endpoint info: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return None

    def get_status(self, namespace_name: str, workgroup_name: str) -> Dict[str, str]:
        """Get status of namespace and workgroup."""
        status = {}

        # Check namespace
        try:
            response = self.client.get_namespace(namespaceName=namespace_name)
            status['namespace'] = response['namespace']['status']
            status['database'] = response['namespace']['dbName']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                status['namespace'] = 'NOT_FOUND'
            else:
                status['namespace'] = f'ERROR: {e}'
        except Exception as e:
            status['namespace'] = f'ERROR: {e}'

        # Check workgroup
        try:
            response = self.client.get_workgroup(workgroupName=workgroup_name)
            status['workgroup'] = response['workgroup']['status']
            endpoint = response['workgroup'].get('endpoint')
            if endpoint:
                status['endpoint'] = f"{endpoint['address']}:{endpoint['port']}"
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                status['workgroup'] = 'NOT_FOUND'
            else:
                status['workgroup'] = f'ERROR: {e}'
        except Exception as e:
            status['workgroup'] = f'ERROR: {e}'

        return status

    def delete_workgroup(self, workgroup_name: str) -> bool:
        """Delete a Redshift Serverless workgroup."""
        print(f"🗑️  Deleting workgroup: {workgroup_name}")

        try:
            self.client.delete_workgroup(workgroupName=workgroup_name)
            print(f"✅ Workgroup deletion initiated: {workgroup_name}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"ℹ️  Workgroup {workgroup_name} does not exist")
                return True
            else:
                print(f"❌ Failed to delete workgroup: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error deleting workgroup: {e}")
            return False

    def wait_for_workgroup_deletion(self, workgroup_name: str, timeout: int = 600) -> bool:
        """Wait for workgroup to be deleted."""
        print(f"⏳ Waiting for workgroup {workgroup_name} to be deleted...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.client.get_workgroup(workgroupName=workgroup_name)
                status = response['workgroup']['status']

                if status == 'DELETING':
                    print(f"ℹ️  Workgroup status: {status}, waiting...")
                    time.sleep(10)
                else:
                    print(f"ℹ️  Workgroup status: {status}")
                    time.sleep(5)

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print(f"✅ Workgroup {workgroup_name} has been deleted")
                    return True
                print(f"❌ Error checking workgroup deletion: {e}")
                return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return False

        print("⏰ Timeout waiting for workgroup deletion")
        return False

    def delete_namespace(self, namespace_name: str) -> bool:
        """Delete a Redshift Serverless namespace."""
        print(f"🗑️  Deleting namespace: {namespace_name}")

        try:
            self.client.delete_namespace(namespaceName=namespace_name)
            print(f"✅ Namespace deletion initiated: {namespace_name}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"ℹ️  Namespace {namespace_name} does not exist")
                return True
            elif error_code == 'ConflictException':
                print(f"⚠️  Cannot delete namespace: {e}")
                print("ℹ️  This usually means workgroup is still being deleted. Try waiting longer.")
                return False
            else:
                print(f"❌ Failed to delete namespace: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error deleting namespace: {e}")
            return False

    def wait_for_namespace_deletion(self, namespace_name: str, timeout: int = 600) -> bool:
        """Wait for namespace to be deleted."""
        print(f"⏳ Waiting for namespace {namespace_name} to be deleted...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = self.client.get_namespace(namespaceName=namespace_name)
                status = response['namespace']['status']

                if status == 'DELETING':
                    print(f"ℹ️  Namespace status: {status}, waiting...")
                    time.sleep(10)
                else:
                    print(f"ℹ️  Namespace status: {status}")
                    time.sleep(5)

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    print(f"✅ Namespace {namespace_name} has been deleted")
                    return True
                print(f"❌ Error checking namespace deletion: {e}")
                return False
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                return False

        print("⏰ Timeout waiting for namespace deletion")
        return False

    def generate_pytest_config(
        self,
        endpoint_info: Dict[str, str],
        database: str = "sqltesting_db",
        admin_user: str = "admin",
        admin_password: str = None
    ) -> str:
        """Generate pytest.ini configuration for testing."""
        if not admin_password:
            admin_password = os.getenv("REDSHIFT_ADMIN_PASSWORD", "CHANGE_ME")

        config = f"""[sql_testing]
adapter = redshift

[sql_testing.redshift]
host = {endpoint_info['host']}
database = {database}
user = {admin_user}
password = {admin_password}
port = {endpoint_info['port']}"""

        return config

    def configure_security_group_for_workgroup(self, workgroup_name: str) -> bool:
        """Configure security group to allow Redshift access."""
        try:
            print(f"🔧 Configuring security group for workgroup: {workgroup_name}")

            # Get workgroup details to find security group IDs
            workgroup_info = self.client.get_workgroup(workgroupName=workgroup_name)
            workgroup = workgroup_info['workgroup']

            # Check if workgroup has custom security groups
            config_parameters = workgroup.get('configParameters', [])
            security_group_ids = []

            # Look for security group configuration
            for param in config_parameters:
                if param.get('parameterKey') == 'security_group_ids':
                    security_group_ids = param.get('parameterValue', '').split(',')
                    break

            # If no custom security groups, use default VPC security group
            if not security_group_ids:
                print("ℹ️  No custom security groups found, using default VPC security group")

                # Get default VPC
                vpcs = self.ec2_client.describe_vpcs(Filters=[{'Name': 'is-default', 'Values': ['true']}])
                if not vpcs['Vpcs']:
                    print("❌ No default VPC found")
                    return False

                default_vpc_id = vpcs['Vpcs'][0]['VpcId']

                # Get default security group for the VPC
                security_groups = self.ec2_client.describe_security_groups(
                    Filters=[
                        {'Name': 'vpc-id', 'Values': [default_vpc_id]},
                        {'Name': 'group-name', 'Values': ['default']}
                    ]
                )

                if not security_groups['SecurityGroups']:
                    print("❌ No default security group found")
                    return False

                security_group_ids = [security_groups['SecurityGroups'][0]['GroupId']]

            # Configure each security group
            for sg_id in security_group_ids:
                self._configure_security_group_rules(sg_id.strip())

            print(f"✅ Security group configuration completed for workgroup: {workgroup_name}")
            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"❌ Workgroup {workgroup_name} not found")
            else:
                print(f"❌ Failed to configure security group: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error configuring security group: {e}")
            return False

    def _configure_security_group_rules(self, security_group_id: str) -> None:
        """Add inbound rules to security group for Redshift access."""
        try:
            print(f"🔐 Configuring security group rules for: {security_group_id}")

            # Check if rule already exists
            sg_info = self.ec2_client.describe_security_groups(GroupIds=[security_group_id])
            existing_rules = sg_info['SecurityGroups'][0]['IpPermissions']

            # Check if Redshift port 5439 is already open
            redshift_rule_exists = False
            for rule in existing_rules:
                if (rule.get('FromPort') == 5439 and
                    rule.get('ToPort') == 5439 and
                    rule.get('IpProtocol') == 'tcp'):
                    redshift_rule_exists = True
                    print("ℹ️  Redshift port 5439 rule already exists in security group")
                    break

            if not redshift_rule_exists:
                # Add inbound rule for Redshift port 5439
                self.ec2_client.authorize_security_group_ingress(
                    GroupId=security_group_id,
                    IpPermissions=[
                        {
                            'IpProtocol': 'tcp',
                            'FromPort': 5439,
                            'ToPort': 5439,
                            'IpRanges': [
                                {
                                    'CidrIp': '0.0.0.0/0',
                                    'Description': 'Redshift access for SQL testing (auto-created)'
                                }
                            ]
                        }
                    ]
                )
                print(f"✅ Added inbound rule for Redshift port 5439 to security group: {security_group_id}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidGroup.Duplicate':
                print("ℹ️  Security group rule already exists")
            else:
                print(f"⚠️  Warning: Could not configure security group {security_group_id}: {e}")
        except Exception as e:
            print(f"⚠️  Warning: Unexpected error configuring security group {security_group_id}: {e}")

    def cleanup_security_group_for_workgroup(self, workgroup_name: str) -> bool:
        """Remove Redshift access rules from security group when destroying workgroup."""
        try:
            print(f"🧹 Cleaning up security group rules for workgroup: {workgroup_name}")

            # Get workgroup details to find security group IDs
            workgroup_info = self.client.get_workgroup(workgroupName=workgroup_name)
            workgroup = workgroup_info['workgroup']

            # Check if workgroup has custom security groups
            config_parameters = workgroup.get('configParameters', [])
            security_group_ids = []

            # Look for security group configuration
            for param in config_parameters:
                if param.get('parameterKey') == 'security_group_ids':
                    security_group_ids = param.get('parameterValue', '').split(',')
                    break

            # If no custom security groups, use default VPC security group
            if not security_group_ids:
                print("ℹ️  No custom security groups found, checking default VPC security group")

                # Get default VPC
                vpcs = self.ec2_client.describe_vpcs(Filters=[{'Name': 'is-default', 'Values': ['true']}])
                if not vpcs['Vpcs']:
                    print("❌ No default VPC found")
                    return False

                default_vpc_id = vpcs['Vpcs'][0]['VpcId']

                # Get default security group for the VPC
                security_groups = self.ec2_client.describe_security_groups(
                    Filters=[
                        {'Name': 'vpc-id', 'Values': [default_vpc_id]},
                        {'Name': 'group-name', 'Values': ['default']}
                    ]
                )

                if not security_groups['SecurityGroups']:
                    print("❌ No default security group found")
                    return False

                security_group_ids = [security_groups['SecurityGroups'][0]['GroupId']]

            # Remove rules from each security group
            cleanup_success = True
            for sg_id in security_group_ids:
                if not self._remove_security_group_rules(sg_id.strip()):
                    cleanup_success = False

            if cleanup_success:
                print(f"✅ Security group cleanup completed for workgroup: {workgroup_name}")
            else:
                print("⚠️  Some security group rules could not be removed")
            return cleanup_success

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceNotFoundException':
                print(f"ℹ️  Workgroup {workgroup_name} not found, skipping security group cleanup")
                return True
            else:
                print(f"❌ Failed to cleanup security group: {e}")
                return False
        except Exception as e:
            print(f"❌ Unexpected error cleaning up security group: {e}")
            return False

    def _remove_security_group_rules(self, security_group_id: str) -> bool:
        """Remove auto-created inbound rules from security group for Redshift access."""
        try:
            print(f"🗑️  Removing Redshift rules from security group: {security_group_id}")

            # Get current security group rules
            sg_info = self.ec2_client.describe_security_groups(GroupIds=[security_group_id])
            existing_rules = sg_info['SecurityGroups'][0]['IpPermissions']

            # Find and remove auto-created Redshift rules (port 5439 with 0.0.0.0/0)
            rules_to_remove = []
            for rule in existing_rules:
                if (rule.get('FromPort') == 5439 and
                    rule.get('ToPort') == 5439 and
                    rule.get('IpProtocol') == 'tcp'):

                    # Check if this rule allows access from 0.0.0.0/0 (our auto-created rule)
                    for ip_range in rule.get('IpRanges', []):
                        if ip_range.get('CidrIp') == '0.0.0.0/0':
                            # Check if this was our auto-created rule
                            description = ip_range.get('Description', '')
                            if 'SQL testing' in description or 'auto-created' in description:
                                # This is our rule, mark it for removal
                                rules_to_remove.append({
                                    'IpProtocol': rule['IpProtocol'],
                                    'FromPort': rule['FromPort'],
                                    'ToPort': rule['ToPort'],
                                    'IpRanges': [ip_range]
                                })
                                print(f"ℹ️  Found auto-created Redshift rule to remove: {description}")
                                break

            # Remove the identified rules
            if rules_to_remove:
                self.ec2_client.revoke_security_group_ingress(
                    GroupId=security_group_id,
                    IpPermissions=rules_to_remove
                )
                print(f"✅ Removed {len(rules_to_remove)} auto-created Redshift rule(s) from security group: {security_group_id}")
            else:
                print(f"ℹ️  No auto-created Redshift rules found to remove in security group: {security_group_id}")

            return True

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'InvalidGroup.NotFound':
                print(f"ℹ️  Security group {security_group_id} not found, skipping rule removal")
                return True
            elif error_code == 'InvalidGroupId.NotFound':
                print("ℹ️  Security group rule not found, may have been already removed")
                return True
            else:
                print(f"⚠️  Warning: Could not remove rules from security group {security_group_id}: {e}")
                return False
        except Exception as e:
            print(f"⚠️  Warning: Unexpected error removing rules from security group {security_group_id}: {e}")
            return False


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(description="Manage Redshift Serverless clusters for testing")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-west-2"),
                       help="AWS region (default: us-west-2)")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create namespace and workgroup")
    create_parser.add_argument("--namespace", default="sql-testing-ns", help="Namespace name (default: sql-testing-ns)")
    create_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")
    create_parser.add_argument("--admin-user", default="admin", help="Admin username")
    create_parser.add_argument("--admin-password", help="Admin password (or use REDSHIFT_ADMIN_PASSWORD env var)")
    create_parser.add_argument("--database", default="sqltesting_db", help="Database name")
    create_parser.add_argument("--base-capacity", type=int, default=8, help="Base capacity for workgroup")
    create_parser.add_argument("--wait", action="store_true", help="Wait for resources to be available")
    create_parser.add_argument("--generate-config", action="store_true", help="Generate pytest.ini config")

    # Status command
    status_parser = subparsers.add_parser("status", help="Get status of resources")
    status_parser.add_argument("--namespace", default="sql-testing-ns", help="Namespace name (default: sql-testing-ns)")
    status_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")

    # Endpoint command
    endpoint_parser = subparsers.add_parser("endpoint", help="Get endpoint information and psql command")
    endpoint_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")
    endpoint_parser.add_argument("--admin-user", default="admin", help="Admin username")
    endpoint_parser.add_argument("--admin-password", help="Admin password (or use REDSHIFT_ADMIN_PASSWORD env var)")
    endpoint_parser.add_argument("--database", default="sqltesting_db", help="Database name")
    endpoint_parser.add_argument("--generate-config", action="store_true", default=True, help="Generate pytest.ini config")
    endpoint_parser.add_argument("--quiet", action="store_true", help="Suppress endpoint information in output")

    # Configure Security Group command
    sg_parser = subparsers.add_parser("configure-sg", help="Configure security group for Redshift access")
    sg_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")

    # Cleanup Security Group command
    cleanup_sg_parser = subparsers.add_parser("cleanup-sg", help="Remove auto-created Redshift security group rules")
    cleanup_sg_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")

    # Destroy command
    destroy_parser = subparsers.add_parser("destroy", help="Destroy workgroup and namespace")
    destroy_parser.add_argument("--namespace", default="sql-testing-ns", help="Namespace name (default: sql-testing-ns)")
    destroy_parser.add_argument("--workgroup", default="sql-testing-wg", help="Workgroup name (default: sql-testing-wg)")
    destroy_parser.add_argument("--skip-sg-cleanup", action="store_true", help="Skip security group rules cleanup")
    # Note: destroy command always waits for deletion completion to ensure proper cleanup

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Initialize manager
    try:
        manager = RedshiftClusterManager(region=args.region)
    except Exception as e:
        print(f"❌ Failed to initialize Redshift manager: {e}")
        sys.exit(1)

    # Execute command
    success = True

    if args.command == "create":
        print(f"🚀 Creating Redshift Serverless resources in {args.region}")

        # Create namespace
        if not manager.create_namespace(
            args.namespace,
            args.admin_user,
            args.admin_password,
            args.database
        ):
            success = False

        # Wait for namespace if requested
        if success and args.wait:
            success = manager.wait_for_namespace(args.namespace)

        # Create workgroup
        if success:
            if not manager.create_workgroup(
                args.workgroup,
                args.namespace,
                args.base_capacity
            ):
                success = False

        # Wait for workgroup if requested
        if success and args.wait:
            success = manager.wait_for_workgroup(args.workgroup)

        # Configure security group for connectivity
        if success:
            print("🔧 Configuring security group for Redshift connectivity...")
            if not manager.configure_security_group_for_workgroup(args.workgroup):
                print("⚠️  Warning: Security group configuration failed. You may need to manually configure security groups.")
                print("ℹ️  Required: Allow inbound TCP traffic on port 5439")

        # Generate config if requested
        if success and args.generate_config:
            endpoint_info = manager.get_endpoint_info(args.workgroup)
            if endpoint_info:
                config = manager.generate_pytest_config(
                    endpoint_info,
                    args.database,
                    args.admin_user,
                    args.admin_password
                )

                print(config)
                print("✅ Sample pytest.ini configuration file")

    elif args.command == "status":
        print("📊 Checking status of Redshift resources")
        status = manager.get_status(args.namespace, args.workgroup)

        print(f"Namespace ({args.namespace}): {status.get('namespace', 'UNKNOWN')}")
        print(f"Workgroup ({args.workgroup}): {status.get('workgroup', 'UNKNOWN')}")
        if 'database' in status:
            print(f"Database: {status['database']}")
        if 'endpoint' in status:
            print(f"Endpoint: {status['endpoint']}")

    elif args.command == "endpoint":
        endpoint_info = manager.get_endpoint_info(args.workgroup, quiet=args.quiet)
        if endpoint_info:
            if not args.quiet:
                print(f"Host: {endpoint_info['host']}")
                print(f"Port: {endpoint_info['port']}")
                print(f"Database: {args.database}")
                print(f"User: {args.admin_user}")

                # Generate psql connection command
                if args.admin_password:
                    password_env = f"PGPASSWORD='{args.admin_password}' "
                    psql_cmd = f"{password_env}psql -h {endpoint_info['host']} -p {endpoint_info['port']} -U {args.admin_user} -d {args.database}"
                else:
                    psql_cmd = f"psql -h {endpoint_info['host']} -p {endpoint_info['port']} -U {args.admin_user} -d {args.database}"

                print("\n🔗 Manual Connection Commands:")
                print("psql command:")
                print(f"  {psql_cmd}")

                if not args.admin_password:
                    print("\nNote: Set REDSHIFT_ADMIN_PASSWORD environment variable for automatic password inclusion")

            if args.generate_config:
                if not args.quiet:
                    print("\n📋 Pytest Configuration:")
                config = manager.generate_pytest_config(
                    endpoint_info,
                    args.database,
                    args.admin_user,
                    args.admin_password
                )
                if not args.quiet:
                    print(config)
                    print("\n✅ Sample pytest.ini configuration generated")
                else:
                    # In quiet mode, just write the config file
                    with open("pytest.ini", "w") as f:
                        f.write(config)
        else:
            success = False

    elif args.command == "configure-sg":
        print(f"🔧 Configuring security group for workgroup: {args.workgroup}")
        success = manager.configure_security_group_for_workgroup(args.workgroup)

    elif args.command == "cleanup-sg":
        print(f"🧹 Cleaning up security group for workgroup: {args.workgroup}")
        success = manager.cleanup_security_group_for_workgroup(args.workgroup)

    elif args.command == "destroy":
        print("🗑️  Destroying Redshift Serverless resources")

        # First, clean up security group rules while workgroup still exists (unless skipped)
        if not args.skip_sg_cleanup:
            print("🧹 Cleaning up security group rules...")
            if not manager.cleanup_security_group_for_workgroup(args.workgroup):
                print("⚠️  Warning: Security group cleanup failed, but continuing with resource deletion")
        else:
            print("ℹ️  Skipping security group cleanup (--skip-sg-cleanup specified)")

        # Delete workgroup first
        if not manager.delete_workgroup(args.workgroup):
            success = False

        # Always wait for workgroup deletion before deleting namespace
        # This is required because namespace deletion will fail if workgroup still exists
        if success:
            print("⏳ Waiting for workgroup deletion to complete (required before namespace deletion)...")
            success = manager.wait_for_workgroup_deletion(args.workgroup)

        # Delete namespace only after workgroup is fully deleted
        if success:
            if not manager.delete_namespace(args.namespace):
                success = False

        # Always wait for namespace deletion to complete (just like workgroup deletion)
        if success:
            print("⏳ Waiting for namespace deletion to complete...")
            success = manager.wait_for_namespace_deletion(args.namespace)

    # Exit with appropriate code
    if success:
        print(f"✅ Command '{args.command}' completed successfully")
        sys.exit(0)
    else:
        print(f"❌ Command '{args.command}' failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
