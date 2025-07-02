"""Test varchar constraints in mocksmith 3.0.1."""

from dataclasses import dataclass

from mocksmith import Integer, Varchar, mockable


# Test startswith and endswith constraints
@mockable
@dataclass
class User:
    user_id: Integer()
    # Varchar with startswith constraint
    username: Varchar(50, startswith="user_")
    # Varchar with endswith constraint
    email: Varchar(100, endswith="@example.com")
    # Varchar with both constraints
    domain: Varchar(50, startswith="https://", endswith=".com")
    # Regular varchar for comparison
    description: Varchar(200)


print("Testing Varchar constraints in mocksmith 3.0.1:")
print("=" * 60)

for i in range(10):
    user = User.mock()
    print(f"\nUser {i + 1}:")
    print(f"  username: {user.username}")
    print(f"  email: {user.email}")
    print(f"  domain: {user.domain}")
    print(f"  description: {user.description[:30]}...")

    # Verify constraints
    assert user.username.startswith("user_"), (
        f"Username doesn't start with 'user_': {user.username}"
    )
    assert user.email.endswith("@example.com"), (
        f"Email doesn't end with '@example.com': {user.email}"
    )
    assert user.domain.startswith("https://"), (
        f"Domain doesn't start with 'https://': {user.domain}"
    )
    assert user.domain.endswith(".com"), f"Domain doesn't end with '.com': {user.domain}"

print("\n✅ Varchar startswith/endswith constraints work!")


# Test with different patterns
@mockable
@dataclass
class Product:
    sku: Varchar(20, startswith="PROD-")
    barcode: Varchar(13, startswith="978")  # ISBN prefix
    serial: Varchar(30, endswith="-US")  # US region suffix


print("\n\nTesting different patterns:")
print("=" * 40)

for i in range(5):
    product = Product.mock()
    print(f"\nProduct {i + 1}:")
    print(f"  SKU: {product.sku}")
    print(f"  Barcode: {product.barcode}")
    print(f"  Serial: {product.serial}")

    assert product.sku.startswith("PROD-")
    assert product.barcode.startswith("978")
    assert product.serial.endswith("-US")

print("\n✅ All varchar constraint patterns work correctly!")
