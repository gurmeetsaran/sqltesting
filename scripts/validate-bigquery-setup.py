#!/usr/bin/env python3
"""
Validate BigQuery setup for CI/CD integration tests.

This script checks if all required environment variables and GCP permissions
are properly configured for running BigQuery integration tests.
"""

import json
import os
import sys
from typing import List, Tuple


def check_environment_variables() -> List[str]:
    """Check if required environment variables are set."""
    required_vars = [
        "GCP_SA_KEY",
        "GCP_PROJECT_ID",
    ]

    optional_vars = [
        ("BIGQUERY_DATABASE", "sqltesting"),
        ("GOOGLE_APPLICATION_CREDENTIALS", "auto-generated"),
    ]

    missing_vars = []

    print("🔍 Checking environment variables...")

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
            print(f"❌ {var}: Not set")
        else:
            # Don't print actual values for security
            if var == "GCP_SA_KEY":
                # Validate JSON format
                try:
                    json.loads(os.getenv(var))
                    print(f"✅ {var}: Set (valid JSON)")
                except json.JSONDecodeError:
                    print(f"❌ {var}: Set but invalid JSON format")
                    missing_vars.append(var)
            else:
                print(f"✅ {var}: Set")

    for var, default in optional_vars:
        value = os.getenv(var, default)
        print(f"✅ {var}: {value} {'(default)' if not os.getenv(var) else ''}")

    return missing_vars


def setup_credentials() -> Tuple[bool, str]:
    """Set up Google Cloud credentials from environment."""
    try:
        gcp_sa_key = os.getenv("GCP_SA_KEY")
        if not gcp_sa_key:
            return False, "GCP_SA_KEY environment variable not set"

        # Write credentials to temporary file
        creds_path = "/tmp/gcp-credentials.json"
        with open(creds_path, "w") as f:
            f.write(gcp_sa_key)

        # Set environment variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

        print(f"✅ Credentials file created: {creds_path}")
        return True, "Credentials setup successful"

    except Exception as e:
        return False, f"Failed to setup credentials: {str(e)}"


def check_bigquery_connection() -> Tuple[bool, str]:
    """Test basic BigQuery connectivity."""
    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import Forbidden, NotFound

        print("\n🔍 Testing BigQuery connectivity...")

        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            return False, "GCP_PROJECT_ID not set"

        client = bigquery.Client(project=project_id)

        # Test basic BigQuery access by listing datasets
        datasets = list(client.list_datasets(max_results=5))
        print(f"✅ Successfully connected to BigQuery (Project: {project_id})")
        print(f"📋 Found {len(datasets)} datasets in project")

        return True, "Connection successful"

    except ImportError:
        return False, "google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery"
    except Forbidden as e:
        return False, f"Access denied. Check service account permissions: {str(e)}"
    except Exception as e:
        return False, f"BigQuery connection error: {str(e)}"


def check_dataset_access() -> Tuple[bool, str]:
    """Test access to specified BigQuery dataset."""
    try:
        from google.cloud import bigquery
        from google.cloud.exceptions import Forbidden, NotFound

        project_id = os.getenv("GCP_PROJECT_ID")
        dataset_name = os.getenv("BIGQUERY_DATABASE", "sqltesting")

        if not project_id:
            return False, "GCP_PROJECT_ID not set"

        dataset_id = f"{project_id}.{dataset_name}"
        print(f"\n🔍 Testing access to BigQuery dataset: {dataset_id}")

        client = bigquery.Client(project=project_id)

        try:
            dataset = client.get_dataset(dataset_id)
            print(f"✅ Dataset '{dataset_id}' exists and is accessible")
            print(f"📍 Location: {dataset.location}")
            print(f"📝 Description: {dataset.description or 'No description'}")

            # Try to list tables
            tables = list(client.list_tables(dataset, max_results=5))
            print(f"📊 Found {len(tables)} tables in dataset")

            return True, "Dataset access verified"

        except NotFound:
            # Try to create the dataset
            print(f"📝 Dataset '{dataset_id}' not found, attempting to create...")

            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "Test dataset for SQL testing library integration tests"

            try:
                created_dataset = client.create_dataset(dataset, timeout=30)
                print(f"✅ Created dataset '{dataset_id}' successfully")
                return True, "Dataset created and accessible"
            except Exception as create_error:
                return False, f"Cannot create dataset: {str(create_error)}"

        except Forbidden as e:
            return False, f"Access denied to dataset: {str(e)}"

    except Exception as e:
        return False, f"Dataset check failed: {str(e)}"


def check_query_permissions() -> Tuple[bool, str]:
    """Test query execution permissions."""
    try:
        from google.cloud import bigquery

        print("\n🔍 Testing BigQuery query permissions...")

        project_id = os.getenv("GCP_PROJECT_ID")
        client = bigquery.Client(project=project_id)

        # Simple test query
        query = "SELECT 1 as test_value, 'CI/CD Test' as test_message"

        print(f"📝 Running test query: {query}")

        query_job = client.query(query)
        results = query_job.result()  # Wait for completion

        # Check results
        rows = list(results)
        if len(rows) == 1 and rows[0].test_value == 1:
            print("✅ Test query executed successfully")
            print("📊 Query returned expected results")
            return True, "Query permissions verified"
        else:
            return False, "Query succeeded but returned unexpected results"

    except Exception as e:
        return False, f"Query test failed: {str(e)}"


def check_service_account_permissions() -> Tuple[bool, str]:
    """Check if service account has required permissions."""
    try:
        from google.cloud import bigquery

        print("\n🔍 Checking service account permissions...")

        project_id = os.getenv("GCP_PROJECT_ID")

        # Test BigQuery permissions
        client = bigquery.Client(project=project_id)

        # Check if we can access BigQuery jobs
        try:
            jobs = list(client.list_jobs(max_results=1))
            print("✅ Can access BigQuery jobs")
        except Exception as e:
            print(f"⚠️  Limited access to BigQuery jobs: {str(e)}")

        # Test table operations
        try:
            dataset_name = os.getenv("BIGQUERY_DATABASE", "sqltesting")
            dataset_id = f"{project_id}.{dataset_name}"
            dataset = client.get_dataset(dataset_id)

            # Try to list tables (requires dataset read permissions)
            tables = list(client.list_tables(dataset, max_results=1))
            print("✅ Can read dataset and tables")

        except Exception as e:
            print(f"⚠️  Limited dataset access: {str(e)}")

        return True, "Service account permissions appear sufficient"

    except Exception as e:
        return False, f"Permission check failed: {str(e)}"


def cleanup_credentials() -> None:
    """Clean up temporary credentials file."""
    try:
        creds_path = "/tmp/gcp-credentials.json"
        if os.path.exists(creds_path):
            os.remove(creds_path)
            print("🧹 Cleaned up temporary credentials file")
    except Exception as e:
        print(f"⚠️  Could not clean up credentials: {str(e)}")


def main() -> None:
    """Main validation function."""
    print("🚀 BigQuery CI/CD Setup Validation")
    print("=" * 50)

    all_checks_passed = True

    try:
        # Check environment variables
        missing_vars = check_environment_variables()
        if missing_vars:
            print(f"\n❌ Missing required environment variables: {', '.join(missing_vars)}")
            print("\nTo fix this, set the missing variables:")
            for var in missing_vars:
                if var == "GCP_SA_KEY":
                    print(f"  export {var}='$(cat path/to/service-account-key.json)'")
                else:
                    print(f"  export {var}=your_value_here")
            all_checks_passed = False

        # Only run GCP checks if credentials are available
        if not missing_vars:
            # Setup credentials
            creds_success, creds_message = setup_credentials()
            if not creds_success:
                print(f"\n❌ Credentials Setup: {creds_message}")
                all_checks_passed = False
                return

            # Check BigQuery connectivity
            bq_success, bq_message = check_bigquery_connection()
            if not bq_success:
                print(f"\n❌ BigQuery Connection: {bq_message}")
                all_checks_passed = False

            # Check dataset access
            dataset_success, dataset_message = check_dataset_access()
            if not dataset_success:
                print(f"\n❌ Dataset Access: {dataset_message}")
                all_checks_passed = False

            # Check query permissions
            if bq_success and dataset_success:
                query_success, query_message = check_query_permissions()
                if not query_success:
                    print(f"\n❌ Query Permissions: {query_message}")
                    all_checks_passed = False

            # Check service account permissions
            if bq_success:
                perms_success, perms_message = check_service_account_permissions()
                if not perms_success:
                    print(f"\n❌ Service Account Permissions: {perms_message}")
                    all_checks_passed = False

    finally:
        cleanup_credentials()

    # Final summary
    print("\n" + "=" * 50)
    if all_checks_passed:
        print("🎉 All checks passed! BigQuery CI/CD setup is ready.")
        print("\nNext steps:")
        print("1. Add sensitive values as GitHub repository secrets:")
        print("   - GCP_SA_KEY, GCP_PROJECT_ID")
        print("2. Add non-sensitive values as GitHub repository variables (optional):")
        print("   - BIGQUERY_DATABASE")
        print("3. Push changes to trigger the CI/CD workflow")
        print("4. Monitor the GitHub Actions logs for test results")
        print("5. Integration tests are located in tests/integration/ folder")
    else:
        print("❌ Some checks failed. Please fix the issues above before proceeding.")
        print("\nFor help, see: .github/BIGQUERY_CICD_SETUP.md")

    sys.exit(0 if all_checks_passed else 1)


if __name__ == "__main__":
    main()
