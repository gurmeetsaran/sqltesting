"""Pytest configuration for integration tests.

This module provides automatic marker addition based on adapter types.
For parametrized tests with adapter_type, it automatically adds the
corresponding adapter marker (e.g., @pytest.mark.athena for adapter_type="athena").
"""

import pytest


def pytest_collection_modifyitems(config, items):
    """Automatically add adapter markers based on parametrized adapter_type.

    This hook runs after test collection and modifies test items to add
    adapter-specific markers based on the adapter_type parameter value.

    For example, a test parametrized with adapter_type="athena" will
    automatically get @pytest.mark.athena added.
    """
    for item in items:
        # Check if the test has parametrized adapter_type
        if hasattr(item, "callspec") and "adapter_type" in item.callspec.params:
            adapter = item.callspec.params["adapter_type"]
            # Add the corresponding marker
            item.add_marker(getattr(pytest.mark, adapter))
