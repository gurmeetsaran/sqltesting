#!/usr/bin/env python3
"""
Validation script for Snowflake CI/CD setup.

This script validates that all required Snowflake environment variables are set
and tests basic connectivity to Snowflake.
"""

import os
import sys
from typing import List


def check_environment_variables() -> List[str]:
    """Check if all required environment variables are set."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE"
    ]

    optional_vars = [
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_ROLE"
    ]

    missing_vars = []

    print("🔍 Checking required environment variables...")
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            print(f"❌ {var}: Not set")
        else:
            print(f"✅ {var}: Set")

    print("\n🔍 Checking optional environment variables...")
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var}: Set ({value})")
        else:
            print(f"⚪ {var}: Not set (optional)")

    return missing_vars


def test_snowflake_connection() -> bool:
    """Test basic Snowflake connectivity."""
    print("\n🔗 Testing Snowflake connection...")

    try:
        import snowflake.connector
    except ImportError:
        print("❌ snowflake-connector-python not installed")
        print("💡 Install with: pip install snowflake-connector-python")
        return False

    # Get connection parameters
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    database = os.getenv("SNOWFLAKE_DATABASE")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
    role = os.getenv("SNOWFLAKE_ROLE")

    try:
        # Test connection
        conn_params = {
            "account": account,
            "user": user,
            "password": password,
            "database": database,
            "schema": schema,
        }

        if warehouse:
            conn_params["warehouse"] = warehouse
        if role:
            conn_params["role"] = role

        print(f"Connecting to account: {account}")
        print(f"Database: {database}, Schema: {schema}")
        print(f"Warehouse: {warehouse}, Role: {role or 'default'}")

        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()

        print("✅ Connection successful!")

        # Test basic query
        cursor.execute("SELECT CURRENT_VERSION() as version")
        result = cursor.fetchone()
        print(f"✅ Snowflake version: {result[0]}")

        # Test current context
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()")
        context = cursor.fetchone()
        print(f"✅ Current context: Database={context[0]}, Schema={context[1]}, Warehouse={context[2]}, Role={context[3]}")

        # Test warehouse usage if specified
        if warehouse:
            try:
                cursor.execute(f"USE WAREHOUSE {warehouse}")
                print(f"✅ Successfully using warehouse: {warehouse}")
            except Exception as e:
                print(f"❌ Failed to use warehouse '{warehouse}': {e}")
                return False

        # Test table creation permissions
        try:
            test_table = "test_sql_library_temp_table"
            cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {test_table} (id INTEGER, name VARCHAR(50))")
            cursor.execute(f"INSERT INTO {test_table} VALUES (1, 'test')")
            cursor.execute(f"SELECT * FROM {test_table}")
            test_result = cursor.fetchone()
            cursor.execute(f"DROP TABLE {test_table}")
            print(f"✅ Table operations successful: {test_result}")
        except Exception as e:
            print(f"❌ Table operations failed: {e}")
            return False

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")

        # Provide specific troubleshooting tips
        error_str = str(e).lower()
        if "incorrect username or password" in error_str:
            print("💡 Check your SNOWFLAKE_USER and SNOWFLAKE_PASSWORD")
        elif "account" in error_str:
            print("💡 Check your SNOWFLAKE_ACCOUNT format (e.g., 'abc12345.us-east-1')")
        elif "warehouse" in error_str or "object does not exist" in error_str:
            print("💡 Check that the warehouse exists and you have USAGE permission")
            print("💡 Run: SHOW WAREHOUSES; to see available warehouses")
            print("💡 Run: SHOW GRANTS TO ROLE <your_role>; to check permissions")
        elif "database" in error_str:
            print("💡 Check that the database exists and you have USAGE permission")

        return False


def validate_pytest_config() -> bool:
    """Validate pytest.ini configuration."""
    print("\n📋 Checking pytest.ini configuration...")

    config_files = ["pytest.ini", "setup.cfg", "tox.ini"]
    config_found = False

    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"✅ Found config file: {config_file}")
            config_found = True

            # Try to read and validate Snowflake section
            try:
                import configparser
                config = configparser.ConfigParser()
                config.read(config_file)

                if "sql_testing.snowflake" in config:
                    print("✅ Found [sql_testing.snowflake] section")
                    snowflake_config = config["sql_testing.snowflake"]

                    required_keys = ["account", "user", "password", "database", "warehouse"]
                    missing_keys = []

                    for key in required_keys:
                        if key not in snowflake_config:
                            missing_keys.append(key)

                    if missing_keys:
                        print(f"❌ Missing required keys in [sql_testing.snowflake]: {missing_keys}")
                        return False
                    else:
                        print("✅ All required Snowflake configuration keys present")

                elif "sql_testing" in config:
                    adapter = config["sql_testing"].get("adapter", "")
                    if adapter == "snowflake":
                        print("⚠️  Adapter set to 'snowflake' but no [sql_testing.snowflake] section found")
                        return False
                    else:
                        print(f"ℹ️  Adapter set to '{adapter}' (not snowflake)")
                else:
                    print("❌ No [sql_testing] section found")
                    return False

            except Exception as e:
                print(f"❌ Error reading config file: {e}")
                return False
            break

    if not config_found:
        print("❌ No pytest configuration file found")
        print("💡 Create pytest.ini with [sql_testing.snowflake] section")
        return False

    return True


def main():
    """Main validation function."""
    print("🚀 Snowflake CI/CD Setup Validation")
    print("=" * 40)

    # Check environment variables
    missing_vars = check_environment_variables()

    if missing_vars:
        print(f"\n❌ Missing required environment variables: {missing_vars}")
        print("💡 Set these environment variables and try again")
        return 1

    # Test connection
    connection_success = test_snowflake_connection()

    # Validate pytest config
    config_success = validate_pytest_config()

    print("\n" + "=" * 40)
    if connection_success and config_success:
        print("🎉 All validations passed! Snowflake setup is ready for CI/CD")
        return 0
    else:
        print("❌ Some validations failed. Please fix the issues above")
        return 1


if __name__ == "__main__":
    sys.exit(main())
