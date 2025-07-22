#!/usr/bin/env python3
"""
Validate DuckDB setup for CI/CD integration tests.

This script checks if DuckDB is properly installed and configured
for running DuckDB integration tests.
"""

import os
import sys
import tempfile
from typing import List


def check_duckdb_installation() -> bool:
    """Check if DuckDB is properly installed."""
    print("🔍 Checking DuckDB installation...")

    try:
        import duckdb
        print(f"✅ DuckDB version: {duckdb.__version__}")
        return True
    except ImportError:
        print("❌ DuckDB not installed. Install with: pip install duckdb")
        return False


def check_basic_functionality() -> bool:
    """Test basic DuckDB functionality."""
    print("🔍 Testing basic DuckDB functionality...")

    try:
        import duckdb

        # Test in-memory database
        conn = duckdb.connect(":memory:")

        # Test basic query
        result = conn.execute("SELECT 1 as test_col").fetchdf()
        if result.iloc[0]['test_col'] == 1:
            print("✅ In-memory database: Working")
        else:
            print("❌ In-memory database: Basic query failed")
            return False

        # Test complex types
        conn.execute("CREATE TABLE test_complex (id INT, data MAP(VARCHAR, INT), scores INT[])")
        conn.execute("INSERT INTO test_complex VALUES (1, MAP{'key1': 10, 'key2': 20}, [85, 90, 78])")
        result = conn.execute("SELECT * FROM test_complex").fetchdf()

        if len(result) == 1:
            print("✅ Complex types (MAP, LIST): Working")
        else:
            print("❌ Complex types: Failed to create/query table")
            return False

        conn.close()
        return True

    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        return False


def check_file_database() -> bool:
    """Test file-based database functionality."""
    print("🔍 Testing file-based database...")

    try:
        import duckdb

        # Create temporary file path for testing (don't create the file yet)
        tmp_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        db_path = tmp_file.name
        tmp_file.close()
        os.unlink(db_path)  # Remove the empty file so DuckDB can create it properly

        try:
            # Test file database
            conn = duckdb.connect(db_path)
            conn.execute("CREATE TABLE test_file (id INT, name VARCHAR)")
            conn.execute("INSERT INTO test_file VALUES (1, 'test')")
            result = conn.execute("SELECT COUNT(*) as count FROM test_file").fetchdf()

            if result.iloc[0]['count'] == 1:
                print("✅ File-based database: Working")
                success = True
            else:
                print("❌ File-based database: Query failed")
                success = False

            conn.close()
            return success

        finally:
            # Clean up temporary file
            try:
                os.unlink(db_path)
            except OSError:
                pass

    except Exception as e:
        print(f"❌ File database test failed: {e}")
        return False


def check_pandas_integration() -> bool:
    """Test pandas DataFrame integration."""
    print("🔍 Testing pandas integration...")

    try:
        import duckdb
        import pandas as pd

        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'scores': [[85, 90], [78, 82], [92, 88]]
        })

        conn = duckdb.connect(":memory:")

        # Register DataFrame
        conn.register("test_df", df)

        # Query DataFrame
        result = conn.execute("SELECT COUNT(*) as count FROM test_df").fetchdf()

        if result.iloc[0]['count'] == 3:
            print("✅ Pandas integration: Working")
            success = True
        else:
            print("❌ Pandas integration: DataFrame query failed")
            success = False

        conn.close()
        return success

    except Exception as e:
        print(f"❌ Pandas integration test failed: {e}")
        return False


def check_struct_types() -> bool:
    """Test struct type functionality."""
    print("🔍 Testing struct types...")

    try:
        import duckdb

        conn = duckdb.connect(":memory:")

        # Test struct creation and querying
        conn.execute("""
            CREATE TABLE test_struct (
                id INT,
                person STRUCT(name VARCHAR, age INT, address STRUCT(city VARCHAR, zip VARCHAR))
            )
        """)

        conn.execute("""
            INSERT INTO test_struct VALUES (
                1,
                {'name': 'Alice', 'age': 30, 'address': {'city': 'NYC', 'zip': '10001'}}
            )
        """)

        result = conn.execute("SELECT person.name, person.address.city FROM test_struct").fetchdf()

        if len(result) == 1 and result.iloc[0]['name'] == 'Alice':
            print("✅ Struct types: Working")
            success = True
        else:
            print("❌ Struct types: Failed to query nested fields")
            success = False

        conn.close()
        return success

    except Exception as e:
        print(f"❌ Struct types test failed: {e}")
        return False


def check_environment_variables() -> List[str]:
    """Check DuckDB-related environment variables."""
    print("🔍 Checking environment variables...")

    # DuckDB doesn't require specific environment variables like cloud databases
    # But we can check for optional configuration
    optional_vars = [
        ("DUCKDB_DATABASE", ":memory: (default for testing)"),
    ]

    missing_vars = []

    for var, default in optional_vars:
        value = os.getenv(var, default)
        print(f"✅ {var}: {value} {'(default)' if not os.getenv(var) else ''}")

    return missing_vars


def run_duckdb_validation() -> bool:
    """Run all DuckDB validation checks."""
    print("🦆 DuckDB Setup Validation")
    print("=" * 40)

    all_checks_passed = True

    # Check installation
    if not check_duckdb_installation():
        all_checks_passed = False

    # Check environment (informational only for DuckDB)
    missing_env_vars = check_environment_variables()

    if all_checks_passed:
        # Test functionality
        checks = [
            check_basic_functionality,
            check_file_database,
            check_pandas_integration,
            check_struct_types,
        ]

        for check in checks:
            try:
                if not check():
                    all_checks_passed = False
            except Exception as e:
                print(f"❌ {check.__name__} failed with exception: {e}")
                all_checks_passed = False

    print("\n" + "=" * 40)
    if all_checks_passed:
        print("✅ All DuckDB validation checks passed!")
        print("🚀 Ready to run DuckDB integration tests")
        return True
    else:
        print("❌ Some DuckDB validation checks failed")
        print("🛠️  Please fix the issues above before running integration tests")
        return False


def main() -> int:
    """Main entry point."""
    success = run_duckdb_validation()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
