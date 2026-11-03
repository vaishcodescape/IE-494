<callout color="blue_bg">
<b>IE-494 · Big Data Processing · Autumn 2026</b><br />Instructor: PM Jat (<code>pm_jat@dau.ac.in</code>) · Student: Aditya Vaish · ID <code>202401401</code><br />Stack: DynamoDB Local 3.3.2, AWS SDK for Java 2.31.0 (<code>dynamodb</code>, <code>dynamodb-enhanced</code>), Java (release 17), Maven · Code in <code>IE-494/lab10</code> · Submission PDF <code>lab10/report/202401401_IE494_Lab10_DynamoDB.pdf</code>
</callout>

<table-of-contents />

---

## Assignment

Take `CUSTOMER`, `ORDERS` and `LINEITEM` from the TPC-H dataset (scale factor 1, generated with `dbgen` as in Lab 08: 150,000 customers, 1,500,000 orders, 6,001,215 line items), design a DynamoDB database for the query load below, populate it, and answer every query in a Java program with one function per query, using the high-level (Enhanced Client) API.

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

```json
{ "pk": "CUST#121", "sk": "PROFILE", "type": "CUSTOMER", "custkey": 121, "name": "Customer#000000121",
  "address": "...", "nationkey": 17, "phone": "27-411-990-2959", "acctbal": 6428.32, "mktsegment": "BUILDING", "comment": "..." }

{ "pk": "CUST#121", "sk": "ORDER#1994-06-26#0005512869", "type": "ORDER", "orderkey": 5512869, "custkey": 121,
  "orderstatus": "F", "totalprice": 133146.02, "orderdate": "1994-06-26", "year": 1994, "orderpriority": "...",
  "clerk": "Clerk#...", "shippriority": 0, "comment": "...",
  "lineitems": [ { "linenumber": 1, "partkey": ..., "suppkey": ..., "quantity": ..., "extendedprice": ..., "discount": ...,
                   "tax": ..., "returnflag": "...", "linestatus": "...", "shipdate": "...", "commitdate": "...",
                   "receiptdate": "...", "shipinstruct": "...", "shipmode": "...", "comment": "..." }, ... ] }
```

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
- <b>Load</b> (`TpchLoader`): `customer.tbl` rows become profile items. `orders.tbl` and `lineitem.tbl` are both written by dbgen in ascending order key, so they are merge-joined in one streaming pass: read an order, take its line items from the head of `lineitem.tbl`, put the list into the order bean. Items go out as `BatchWriteItem` requests of 25 (`DynamoDbEnhancedClient.batchWriteItem` with a `WriteBatch` per request), 8 writer threads, unprocessed items re-sent with backoff.
- <b>Queries</b> (`Queries`, one method each): `DynamoDbTable.getItem(Key)`, `DynamoDbTable.query(QueryConditional.sortBeginsWith(...))` and `DynamoDbIndex.query(QueryConditional.keyEqualTo(...))`. Results are iterated page by page; the program counts the pages and the elapsed time. Query 6 is two Queries plus a set difference of part keys in Java; Query 7 sums `totalprice` and cross-checks it against Σ extendedprice × (1 − discount) × (1 + tax) of the embedded line items.

## Results

| | value |
| --- | --- |
| customer.tbl rows → profile items | 150,000 |
| orders.tbl rows → order items | 1,500,000 |
| lineitem.tbl rows (embedded in the orders) | 6,001,215 |
| profile items put | 150,000 in 66.9 s (2,242 items/s) |
| order items put | 1,500,000 in 1901.6 s (789 items/s) |
| items in the table | 1,650,000 |
| batch retries | 0 |
| describe-table item count / size | 1,650,000 / 1,881,362,746 bytes (1794.2 MiB) |

Requests and wall time of each query (customer 121, order 5512869, year 1994):

| query | requests | time |
| --- | --- | --- |
| Q1: retrieve the profile of customer 121 | 1 GetItem | 98.1 ms |
| Q2: all orders of customer 121 | 1 Query page(s) | 44.4 ms |
| Q3: all line items of order 5512869 | 1 index Query page(s) + 1 GetItem | 11.4 ms |
| Q4: order history of customer 121 (profile + each order with its line items) | 1 GetItem + 1 Query page(s) | 18.3 ms |
| Q5: items ordered by customer 121 in 1994 | 1 Query page(s) | 11.7 ms |
| Q6: items customer 121 ordered in 1993 but not in 1994 | 2 Query page(s) | 16.1 ms |
| Q7: total value of the orders of customer 121 in 1994 | 1 Query page(s) | 13.3 ms |

- <b>Q1</b> is a single key lookup. <b>Q2, Q5, Q7</b> are one single-partition Query each, and the year queries read only the orders of that year because the year is a prefix of the sort key. <b>Q4</b> needs one extra GetItem for the profile.
- <b>Q3</b> costs one index Query plus one GetItem, the price of a KEYS_ONLY projection; projecting ALL would make it one request at the cost of duplicating every order (with its line items) into the index.
- <b>Q6</b> cannot be a single DynamoDB request (a set difference is not a key condition); it is two year Queries and a `TreeSet` difference in the client. TPC-H part keys are random over 200,000 parts, so the two years share no part and the answer is every part of year-1.
- <b>Q7</b>: the sum of `totalprice` (498,215.67) and the value recomputed from the embedded line items (498,215.88) differ by 21 cents because dbgen truncates each line's extendedprice × (1 − discount) × (1 + tax) to whole cents before adding it into O_TOTALPRICE; both numbers come from the same single Query.
- Load throughput is bounded by DynamoDB Local (one SQLite database behind the HTTP endpoint), not by the client; profile items are small and order items carry 1–7 line items each.

## Files and how to run

```bash
bash lab10/run_all.sh [custkey] [orderkey] [year]      # DynamoDB Local + dbgen data if missing, build, create, load, describe, q1..q7
python3 lab10/report/generate_report_pdf.py              # single PDF: write-up, all outputs, highlighted source
```

- `lab10/output/01_create.txt` … `04_queries.txt`: output of each step (`run_log.txt` is all four together)
- `lab10/report/202401401_IE494_Lab10_DynamoDB.pdf`: the submission PDF
