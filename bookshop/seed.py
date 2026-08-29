"""Create and load the bookshop tables in the REST catalog.

Deterministic: the same rows every time, so the figures in the book
reproduce rather than drift.
"""
import datetime as dt
import pyarrow as pa

from bookshop.catalog import rest_catalog
from bookshop.schema import ORDERS, BOOKS, CUSTOMERS, NAMESPACE

TITLES = ["Dune", "Neuromancer", "Blindsight", "Anathem"]
AUTHORS = ["Herbert", "Gibson", "Watts", "Stephenson"]
GENRES = ["scifi", "cyberpunk", "scifi", "scifi"]
COUNTRIES = ["GB", "US", "DE"]
N_ORDERS = 50_000


def orders_batch(start: int, n: int, schema, status: str | None = None) -> pa.Table:
    """One batch of orders.

    `status` forces every row in the batch to one value, which is how chapter 3
    lays the data out so that per-file statistics can prune on a string column.
    Left as None, statuses interleave the way the full seed does.
    """
    base = dt.datetime(2026, 1, 1)
    return pa.table(
        {
            "order_id": pa.array(range(start, start + n), pa.int64()),
            "customer_id": pa.array([(i % 5000) + 1 for i in range(n)], pa.int64()),
            "title": pa.array([TITLES[i % 4] for i in range(n)]),
            "country": pa.array([COUNTRIES[i % 3] for i in range(n)]),
            "quantity": pa.array([(i % 3) + 1 for i in range(n)], pa.int32()),
            "amount": pa.array([(i % 90) * 1.5 for i in range(n)], pa.float64()),
            "status": pa.array([status or ("shipped" if i % 7 == 0 else "placed") for i in range(n)]),
            "ordered_at": pa.array(
                [base + dt.timedelta(minutes=i // 40) for i in range(n)],
                pa.timestamp("us"),
            ),
        },
        schema=schema.as_arrow(),
    )


def main() -> None:
    cat = rest_catalog()
    cat.create_namespace_if_not_exists(NAMESPACE)

    for name, schema in (("orders", ORDERS), ("books", BOOKS), ("customers", CUSTOMERS)):
        ident = f"{NAMESPACE}.{name}"
        if (NAMESPACE, name) in cat.list_tables(NAMESPACE):
            cat.drop_table(ident)
        cat.create_table(ident, schema=schema)

    books = cat.load_table(f"{NAMESPACE}.books")
    books.append(
        pa.table(
            {
                "book_id": pa.array(range(1, 5), pa.int64()),
                "title": pa.array(TITLES),
                "author": pa.array(AUTHORS),
                "genre": pa.array(GENRES),
                "price": pa.array([9.99, 7.50, 6.25, 12.00], pa.float64()),
            },
            schema=BOOKS.as_arrow(),
        )
    )

    customers = cat.load_table(f"{NAMESPACE}.customers")
    customers.append(
        pa.table(
            {
                "customer_id": pa.array(range(1, 5001), pa.int64()),
                "name": pa.array([f"Customer {i}" for i in range(1, 5001)]),
                "country": pa.array([COUNTRIES[i % 3] for i in range(5000)]),
            },
            schema=CUSTOMERS.as_arrow(),
        )
    )

    orders = cat.load_table(f"{NAMESPACE}.orders")
    orders.append(orders_batch(1, N_ORDERS, ORDERS))

    print(f"  books     {books.scan().to_arrow().num_rows:>7} rows")
    print(f"  customers {customers.scan().to_arrow().num_rows:>7} rows")
    print(f"  orders    {orders.scan().to_arrow().num_rows:>7} rows "
          f"in {orders.inspect.files().num_rows} data file(s)")


if __name__ == "__main__":
    main()
