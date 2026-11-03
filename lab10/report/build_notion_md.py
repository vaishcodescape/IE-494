#!/usr/bin/env python3
"""Emit the Notion page body for Lab 10 in three chunks (write-up, outputs, source code)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "output"
REPORT = ROOT / "report"
JAVA = ROOT / "src/main/java/pmj/bdp/dynamo/lab10"

SOURCES = [
    ("Lab10.java", JAVA / "Lab10.java", "java"),
    ("Queries.java", JAVA / "Queries.java", "java"),
    ("TpchLoader.java", JAVA / "TpchLoader.java", "java"),
    ("Schema.java", JAVA / "Schema.java", "java"),
    ("Keys.java", JAVA / "Keys.java", "java"),
    ("Customer.java", JAVA / "Customer.java", "java"),
    ("Order.java", JAVA / "Order.java", "java"),
    ("LineItem.java", JAVA / "LineItem.java", "java"),
    ("Dynamo.java", JAVA / "Dynamo.java", "java"),
    ("pom.xml", ROOT / "pom.xml", "xml"),
    ("run_all.sh", ROOT / "run_all.sh", "bash"),
]


def grab(text: str, pattern: str, default: str = "?") -> str:
    m = re.search(pattern, text, flags=re.M)
    return m.group(1) if m else default


def query_sections(text: str) -> list[tuple[str, str]]:
    parts = re.split(r"^======== (Q\d: .*?) ========$", text, flags=re.M)
    return [(parts[i], parts[i + 1].strip("\n")) for i in range(1, len(parts), 2)]


def fence(lang: str, body: str) -> str:
    return f"```{lang}\n{body.rstrip()}\n```"


def toggle(title: str, body: str) -> str:
    return f'<toggle heading="h3">\n{title}\n\n{body}\n</toggle>'


def main() -> None:
    read = lambda name: (OUT / name).read_text() if (OUT / name).exists() else ""
    create, load, describe, queries = read("01_create.txt"), read("02_load.txt"), read("03_describe.txt"), read("04_queries.txt")
    customers = grab(load, r"customer\.tbl rows\s*:\s*([\d,]+)")
    orders = grab(load, r"orders\.tbl rows\s*:\s*([\d,]+)")
    lines = grab(load, r"lineitem\.tbl rows\s*:\s*([\d,]+)")
    prof = grab(load, r"profile items put\s*:\s*(.*)$")
    ords = grab(load, r"order items put\s*:\s*(.*)$")
    total_items = grab(load, r"items in table\s*:\s*([\d,]+)")
    retries = grab(load, r"batch retries\s*:\s*([\d,]+)")
    threads = grab(load, r"writer threads\s*:\s*(\d+)")
    item_count = grab(describe, r"item count\s*:\s*([\d,]+)")
    table_size = grab(describe, r"table size\s*:\s*(.*)$")
    qsecs = query_sections(queries)
    timing = []
    for title, body in qsecs:
        m = re.search(r"^\((.*?), ([\d.]+) ms\)$", body, flags=re.M)
        timing.append((title, m.group(1) if m else "?", (m.group(2) + " ms") if m else "?"))

    part1 = f"""<callout color="blue_bg">
<b>IE-494 · Big Data Processing · Autumn 2026</b><br />Instructor: PM Jat (<code>pm_jat@dau.ac.in</code>) · Student: Aditya Vaish · ID <code>202401401</code><br />Stack: DynamoDB Local 3.3.2, AWS SDK for Java 2.31.0 (<code>dynamodb</code>, <code>dynamodb-enhanced</code>), Java (release 17), Maven · Code in <code>IE-494/lab10</code> · Submission PDF <code>lab10/report/202401401_IE494_Lab10_DynamoDB.pdf</code>
</callout>

<table-of-contents />

---

## Assignment

Take `CUSTOMER`, `ORDERS` and `LINEITEM` from the TPC-H dataset (scale factor 1, generated with `dbgen` as in Lab 08: {customers} customers, {orders} orders, {lines} line items), design a DynamoDB database for the query load below, populate it, and answer every query in a Java program with one function per query, using the high-level (Enhanced Client) API.

1. Retrieve a customer's profile.
2. Retrieve all orders for a customer.
3. Retrieve all line items belonging to an order.
4. Retrieve the details required to display a customer's order history.
5. List all items ordered by a customer in a year.
6. List all items a customer ordered in the previous year (year-1) but has not bought this year.
7. Show the total value of a customer's orders in a given year.

## Database design

Every query is driven by one customer (1, 2, 4, 5, 6, 7) or one order (3), so the data is modelled as <b>aggregates</b> in a <b>single table</b> `TPCH` with a composite primary key `pk` (partition, string) and `sk` (sort, string).

- A customer's partition `CUST#<custkey>` holds the profile item (`sk = PROFILE`) and one item per order.
- An order item's sort key `ORDER#<orderdate>#<orderkey>` starts with the date, so a customer's orders are stored in date order and "orders of year Y" is just `begins_with(sk, "ORDER#Y")`. The 10-digit zero-padded order key at the end keeps the key unique.
- The line items of an order are <b>embedded</b> in the order item as a list of maps (the `CORDERS` aggregate of Lab 08). TPC-H orders have 1–7 line items, so an item is at most a few KB, far below the 400 KB limit, and every order-level query gets the line items for free.
- Query 3 is addressed by order key only, so a <b>global secondary index</b> `OrderIndex` with partition key `orderkey` (projection `KEYS_ONLY`) maps an order key to the order's (pk, sk); a `GetItem` then returns the order with its line items. KEYS_ONLY keeps the index small (pk, sk, orderkey per order).
- No `Scan` is needed for any of the seven queries; each one is a key lookup or a single-partition `Query`.

| item | pk | sk | other attributes |
| --- | --- | --- | --- |
| customer profile | `CUST#<custkey>` | `PROFILE` | type, custkey, name, address, nationkey, phone, acctbal, mktsegment, comment |
| order with its line items | `CUST#<custkey>` | `ORDER#<orderdate>#<orderkey 10 digits>` | type, orderkey, custkey, orderstatus, totalprice, orderdate, year, orderpriority, clerk, shippriority, comment, `lineitems` = list of maps (linenumber, partkey, suppkey, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment) |

Items as stored:

{fence("json", '''{ "pk": "CUST#121", "sk": "PROFILE", "type": "CUSTOMER", "custkey": 121, "name": "Customer#000000121",
  "address": "...", "nationkey": 17, "phone": "27-411-990-2959", "acctbal": 6428.32, "mktsegment": "BUILDING", "comment": "..." }

{ "pk": "CUST#121", "sk": "ORDER#1994-06-26#0005512869", "type": "ORDER", "orderkey": 5512869, "custkey": 121,
  "orderstatus": "F", "totalprice": 133146.02, "orderdate": "1994-06-26", "year": 1994, "orderpriority": "...",
  "clerk": "Clerk#...", "shippriority": 0, "comment": "...",
  "lineitems": [ { "linenumber": 1, "partkey": ..., "suppkey": ..., "quantity": ..., "extendedprice": ..., "discount": ...,
                   "tax": ..., "returnflag": "...", "linestatus": "...", "shipdate": "...", "commitdate": "...",
                   "receiptdate": "...", "shipinstruct": "...", "shipmode": "...", "comment": "..." }, ... ] }''')}

<b>Query load → DynamoDB operations</b>

| # | query | operation | requests |
| --- | --- | --- | --- |
| 1 | Retrieve a customer's profile | `GetItem` (CUST#c, PROFILE) | 1 |
| 2 | Retrieve all orders for a customer | `Query` pk = CUST#c, sk `begins_with` ORDER# | 1 (paged) |
| 3 | Retrieve all line items of an order | `Query` GSI OrderIndex orderkey = o → (pk, sk); `GetItem` the order; the line items are inside it | 2 |
| 4 | Customer's order history | `GetItem` profile + `Query` pk = CUST#c, sk begins_with ORDER# (every order already carries its line items) | 2 |
| 5 | Items ordered by a customer in year Y | `Query` pk = CUST#c, sk begins_with ORDER#Y; collect the embedded line items | 1 |
| 6 | Items ordered in Y-1 but not in Y | `Query` ORDER#(Y-1) and `Query` ORDER#Y; set difference of partkeys in the program | 2 |
| 7 | Total value of a customer's orders in year Y | `Query` pk = CUST#c, sk begins_with ORDER#Y; sum O_TOTALPRICE | 1 |

## Setup

- <b>DynamoDB Local</b> (`dynamodb_local_latest`, version 3.3.2) started as in the setup sheet: `java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -port 8000 -dbPath lab10/db`. The program connects with endpoint `http://localhost:8000`, region `ap-south-1`, access key / secret `1234` / `1234`.
- <b>Data:</b> `customer.tbl`, `orders.tbl`, `lineitem.tbl` from `dbgen -s 1` (the 1 GB TPC-H population used in Labs 08 and 09).
- <b>Dependencies</b> (from the lab sheet): AWS SDK BOM 2.31.0, `dynamodb`, `dynamodb-enhanced`, `slf4j-simple` 1.7.36.
- <b>Run:</b> `bash lab10/run_all.sh` → create table, load, describe, then the seven queries for customer 121, its order 5512869 and year 1994. Customer 121 was chosen because it has 15 orders in two consecutive years (3 in 1993, 5 in 1994), which exercises queries 5–7.

## How it works

- <b>Model classes</b> (`Customer`, `Order`, `LineItem`): `@DynamoDbBean` POJOs. `pk`/`sk` carry `@DynamoDbPartitionKey`/`@DynamoDbSortKey`; `Order.orderkey` carries `@DynamoDbSecondaryPartitionKey(indexNames = "OrderIndex")`; `Order.lineitems` is a `List<LineItem>`, which the Enhanced Client maps to a DynamoDB list of maps. Money columns are `BigDecimal` (DynamoDB numbers).
- <b>Create</b> (`Schema.create`): `DynamoDbTable<Order>.createTable(...)` with an `EnhancedGlobalSecondaryIndex` for `OrderIndex`; the key schema and attribute definitions come from the bean annotations. On-demand (PAY_PER_REQUEST) billing.
- <b>Load</b> (`TpchLoader`): `customer.tbl` rows become profile items. `orders.tbl` and `lineitem.tbl` are both written by dbgen in ascending order key, so they are merge-joined in one streaming pass: read an order, take its line items from the head of `lineitem.tbl`, put the list into the order bean. Items go out as `BatchWriteItem` requests of 25 (`DynamoDbEnhancedClient.batchWriteItem` with a `WriteBatch` per request), {threads} writer threads, unprocessed items re-sent with backoff.
- <b>Queries</b> (`Queries`, one method each): `DynamoDbTable.getItem(Key)`, `DynamoDbTable.query(QueryConditional.sortBeginsWith(...))` and `DynamoDbIndex.query(QueryConditional.keyEqualTo(...))`. Results are iterated page by page; the program counts the pages and the elapsed time. Query 6 is two Queries plus a set difference of part keys in Java; Query 7 sums `totalprice` and cross-checks it against Σ extendedprice × (1 − discount) × (1 + tax) of the embedded line items.

## Results

| | value |
| --- | --- |
| customer.tbl rows → profile items | {customers} |
| orders.tbl rows → order items | {orders} |
| lineitem.tbl rows (embedded in the orders) | {lines} |
| profile items put | {prof} |
| order items put | {ords} |
| items in the table | {total_items} |
| batch retries | {retries} |
| describe-table item count / size | {item_count} / {table_size} |

Requests and wall time of each query (customer 121, order 5512869, year 1994):

| query | requests | time |
| --- | --- | --- |
""" + "\n".join(f"| {t} | {r} | {ms} |" for t, r, ms in timing) + """

- <b>Q1</b> is a single key lookup. <b>Q2, Q5, Q7</b> are one single-partition Query each, and the year queries read only the orders of that year because the year is a prefix of the sort key. <b>Q4</b> needs one extra GetItem for the profile.
- <b>Q3</b> costs one index Query plus one GetItem, the price of a KEYS_ONLY projection; projecting ALL would make it one request at the cost of duplicating every order (with its line items) into the index.
- <b>Q6</b> cannot be a single DynamoDB request (a set difference is not a key condition); it is two year Queries and a `TreeSet` difference in the client. TPC-H part keys are random over 200,000 parts, so the two years share no part and the answer is every part of year-1.
- <b>Q7</b>: the sum of `totalprice` (498,215.67) and the value recomputed from the embedded line items (498,215.88) differ by 21 cents because dbgen truncates each line's extendedprice × (1 − discount) × (1 + tax) to whole cents before adding it into O_TOTALPRICE; both numbers come from the same single Query.
- Load throughput is bounded by DynamoDB Local (one SQLite database behind the HTTP endpoint), not by the client; profile items are small and order items carry 1–7 line items each.

## Files and how to run

""" + fence("bash", """bash lab10/run_all.sh [custkey] [orderkey] [year]      # DynamoDB Local + dbgen data if missing, build, create, load, describe, q1..q7
python3 lab10/report/generate_report_pdf.py              # single PDF: write-up, all outputs, highlighted source""") + """

- `lab10/output/01_create.txt` … `04_queries.txt`: output of each step (`run_log.txt` is all four together)
- `lab10/report/202401401_IE494_Lab10_DynamoDB.pdf`: the submission PDF
"""

    outputs = [
        "## Terminal output",
        toggle("01_create.txt: create table TPCH + GSI OrderIndex", fence("txt", create)),
        toggle("02_load.txt: load customer.tbl, orders.tbl, lineitem.tbl", fence("txt", load)),
        toggle("03_describe.txt: describe-table after the load", fence("txt", describe)),
    ]
    for title, body in qsecs:
        outputs.append(toggle(title, fence("txt", body)))
    part2 = "\n\n".join(outputs) + "\n"

    code = ["## Source code", "All of `lab10/`, package `pmj.bdp.dynamo.lab10`."]
    for title, path, lang in SOURCES:
        code.append(toggle(title, fence(lang, path.read_text())))
    part3 = "\n\n".join(code) + "\n"

    for name, text in (("notion_part1.md", part1), ("notion_part2.md", part2), ("notion_part3.md", part3)):
        (REPORT / name).write_text(text)
        print(f"{name}: {len(text):,} chars")


if __name__ == "__main__":
    main()
