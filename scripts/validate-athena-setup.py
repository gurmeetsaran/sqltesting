#!/usr/bin/env python3
"""
Validate Athena setup for CI/CD integration tests.

This script checks if all required environment variables and AWS permissions
are properly configured for running Athena integration tests.
"""

import os
import sys
from typing import List, Tuple


def check_environment_variables() -> List[str]:
    """Check if required environment variables are set."""
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ATHENA_DATABASE",
        "AWS_ATHENA_OUTPUT_LOCATION",
    ]

    optional_vars = [
        ("AWS_REGION", "us-west-2"),
    ]

    missing_vars = []

    print("🔍 Checking environment variables...")

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
            print(f"❌ {var}: Not set")
        else:
            # Don't print actual values for security
            print(f"✅ {var}: Set")

    for var, default in optional_vars:
        value = os.getenv(var, default)
        print(f"✅ {var}: {value} {'(default)' if not os.getenv(var) else ''}")

    return missing_vars


def check_aws_connection() -> Tuple[bool, str]:
    """Test basic AWS connectivity."""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        print("\n🔍 Testing AWS connectivity...")

        region = os.getenv("AWS_REGION", "us-west-2")
        client = boto3.client("athena", region_name=region)

        # Test basic Athena access
        response = client.list_work_groups()
        print("✅ Successfully connected to AWS Athena")

        work_groups = [wg["Name"] for wg in response.get("WorkGroups", [])]
        print(f"📋 Available work groups: {', '.join(work_groups[:3])}")

        return True, "Connection successful"

    except ImportError:
        return False, "boto3 not installed. Run: pip install boto3"
    except NoCredentialsError:
        return False, "AWS credentials not found or invalid"
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AccessDenied":
            return False, "Access denied. Check IAM permissions for Athena"
        else:
            return False, f"AWS error: {error_code} - {e.response['Error']['Message']}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def check_s3_access() -> Tuple[bool, str]:
    """Test S3 access for Athena query results."""
    try:
        from urllib.parse import urlparse

        import boto3
        from botocore.exceptions import ClientError

        output_location = os.getenv("AWS_ATHENA_OUTPUT_LOCATION")
        if not output_location:
            return False, "AWS_ATHENA_OUTPUT_LOCATION not set"

        print(f"\n🔍 Testing S3 access for: {output_location}")

        # Parse S3 URL
        parsed = urlparse(output_location)
        if parsed.scheme != "s3":
            return False, f"Invalid S3 URL format: {output_location}"

        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")

        region = os.getenv("AWS_REGION", "us-west-2")
        s3_client = boto3.client("s3", region_name=region)

        # Test bucket access
        try:
            s3_client.head_bucket(Bucket=bucket)
            print(f"✅ S3 bucket '{bucket}' is accessible")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False, f"S3 bucket '{bucket}' does not exist"
            else:
                return (
                    False,
                    f"Cannot access S3 bucket '{bucket}': {e.response['Error']['Message']}",
                )

        # Test write permissions by attempting to list objects
        try:
            s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            print(f"✅ Can access S3 path: s3://{bucket}/{prefix}")
        except ClientError as e:
            return False, f"Cannot access S3 path: {e.response['Error']['Message']}"

        return True, "S3 access verified"

    except Exception as e:
        return False, f"S3 check failed: {str(e)}"


def check_athena_database() -> Tuple[bool, str]:
    """Test access to specified Athena database."""
    try:
        import boto3
        from botocore.exceptions import ClientError

        database = os.getenv("AWS_ATHENA_DATABASE")
        if not database:
            return False, "AWS_ATHENA_DATABASE not set"

        print(f"\n🔍 Testing access to Athena database: {database}")

        region = os.getenv("AWS_REGION", "us-west-2")

        # Use Glue client to check database existence
        glue_client = boto3.client("glue", region_name=region)

        try:
            response = glue_client.get_database(Name=database)
            print(f"✅ Database '{database}' exists and is accessible")

            # Try to list tables
            tables_response = glue_client.get_tables(
                DatabaseName=database, MaxResults=5
            )
            table_count = len(tables_response.get("TableList", []))
            print(f"📊 Found {table_count} tables in database")

            return True, "Database access verified"

        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityNotFoundException":
                return False, f"Database '{database}' does not exist"
            else:
                return (
                    False,
                    f"Cannot access database: {e.response['Error']['Message']}",
                )

    except Exception as e:
        return False, f"Database check failed: {str(e)}"


def run_sample_query() -> Tuple[bool, str]:
    """Run a simple test query to verify end-to-end functionality."""
    try:
        import time

        import boto3

        print("\n🔍 Running sample Athena query...")

        region = os.getenv("AWS_REGION", "us-west-2")
        database = os.getenv("AWS_ATHENA_DATABASE")
        output_location = os.getenv("AWS_ATHENA_OUTPUT_LOCATION")

        client = boto3.client("athena", region_name=region)

        # Simple test query
        query = "SELECT 1 as test_value, 'CI/CD Test' as test_message"

        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )

        query_id = response["QueryExecutionId"]
        print(f"📝 Started query: {query_id}")

        # Wait for completion (max 30 seconds)
        for _ in range(30):
            status_response = client.get_query_execution(QueryExecutionId=query_id)
            status = status_response["QueryExecution"]["Status"]["State"]

            if status == "SUCCEEDED":
                print("✅ Sample query executed successfully")

                # Get results
                results = client.get_query_results(QueryExecutionId=query_id)
                if results["ResultSet"]["Rows"]:
                    print("📊 Query returned expected results")
                    return True, "End-to-end test successful"
                else:
                    return False, "Query succeeded but returned no results"

            elif status in ["FAILED", "CANCELLED"]:
                error_msg = status_response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown error"
                )
                return False, f"Query failed: {error_msg}"

            time.sleep(1)

        return False, "Query timed out after 30 seconds"

    except Exception as e:
        return False, f"Sample query failed: {str(e)}"


def main() -> None:
    """Main validation function."""
    print("🚀 Athena CI/CD Setup Validation")
    print("=" * 50)

    all_checks_passed = True

    # Check environment variables
    missing_vars = check_environment_variables()
    if missing_vars:
        print(f"\n❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("\nTo fix this, set the missing variables:")
        for var in missing_vars:
            print(f"  export {var}=your_value_here")
        all_checks_passed = False

    # Only run AWS checks if credentials are available
    if not missing_vars:
        # Check AWS connectivity
        aws_success, aws_message = check_aws_connection()
        if not aws_success:
            print(f"\n❌ AWS Connection: {aws_message}")
            all_checks_passed = False

        # Check S3 access
        s3_success, s3_message = check_s3_access()
        if not s3_success:
            print(f"\n❌ S3 Access: {s3_message}")
            all_checks_passed = False

        # Check Athena database
        db_success, db_message = check_athena_database()
        if not db_success:
            print(f"\n❌ Database Access: {db_message}")
            all_checks_passed = False

        # Run sample query if all basic checks pass
        if aws_success and s3_success and db_success:
            query_success, query_message = run_sample_query()
            if not query_success:
                print(f"\n❌ Sample Query: {query_message}")
                all_checks_passed = False

    # Final summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("🎉 All checks passed! Athena CI/CD setup is ready.")
        print("\nNext steps:")
        print("1. Add sensitive values as GitHub repository secrets:")
        print(
            "   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ATHENA_OUTPUT_LOCATION"
        )
        print("2. Add non-sensitive values as GitHub repository variables:")
        print("   - AWS_ATHENA_DATABASE, AWS_REGION")
        print("3. Push changes to trigger the CI/CD workflow")
        print("4. Monitor the GitHub Actions logs for test results")
        print("5. Integration tests are located in tests/integration/ folder")
    else:
        print("❌ Some checks failed. Please fix the issues above before proceeding.")
        print("\nFor help, see: .github/ATHENA_CICD_SETUP.md")

    sys.exit(0 if all_checks_passed else 1)


if __name__ == "__main__":
    main()
