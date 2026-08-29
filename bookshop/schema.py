"""The canonical schemas. Field IDs here match the ones printed in the book."""
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, IntegerType, DoubleType, StringType, TimestampType,
)

# chapters 3 and 6 print these exact field IDs; chapter 7 evolves them
ORDERS = Schema(
    NestedField(1, "order_id", LongType(), required=True),
    NestedField(2, "customer_id", LongType(), required=True),
    NestedField(3, "title", StringType(), required=False),
    NestedField(4, "country", StringType(), required=False),
    NestedField(5, "quantity", IntegerType(), required=False),
    NestedField(6, "amount", DoubleType(), required=False),
    NestedField(7, "status", StringType(), required=False),
    NestedField(8, "ordered_at", TimestampType(), required=True),
)

BOOKS = Schema(
    NestedField(1, "book_id", LongType(), required=True),
    NestedField(2, "title", StringType(), required=False),
    NestedField(3, "author", StringType(), required=False),
    NestedField(4, "genre", StringType(), required=False),
    NestedField(5, "price", DoubleType(), required=False),
)

CUSTOMERS = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "country", StringType(), required=False),
)

NAMESPACE = "bookshop"
