#!/usr/bin/env python3
"""Build the single submission PDF for Lab 10: design, outputs of every query, full highlighted source."""

from __future__ import annotations

import html
import re
import subprocess
from pathlib import Path

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import BashLexer, JavaLexer, XmlLexer

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "output"
REPORT = ROOT / "report"
PDF = REPORT / "202401401_IE494_Lab10_DynamoDB.pdf"
HTML_PATH = REPORT / "202401401_IE494_Lab10_DynamoDB.html"
JAVA = ROOT / "src/main/java/pmj/bdp/dynamo/lab10"

SOURCES = [
    ("Lab10.java", JAVA / "Lab10.java", JavaLexer()),
    ("Queries.java", JAVA / "Queries.java", JavaLexer()),
    ("TpchLoader.java", JAVA / "TpchLoader.java", JavaLexer()),
    ("Schema.java", JAVA / "Schema.java", JavaLexer()),
    ("Keys.java", JAVA / "Keys.java", JavaLexer()),
    ("Customer.java", JAVA / "Customer.java", JavaLexer()),
    ("Order.java", JAVA / "Order.java", JavaLexer()),
    ("LineItem.java", JAVA / "LineItem.java", JavaLexer()),
    ("Dynamo.java", JAVA / "Dynamo.java", JavaLexer()),
    ("pom.xml", ROOT / "pom.xml", XmlLexer()),
    ("run_all.sh", ROOT / "run_all.sh", BashLexer()),
]


def esc(s: str) -> str:
    return html.escape(s)


def table(headers: list[str], rows: list[list[str]], numeric: set[int] | None = None, raw: bool = False) -> str:
    numeric = numeric or set()
    cell = (lambda c: c) if raw else esc
    th = "".join(f"<th>{cell(h)}</th>" for h in headers)
    body = []
    for row in rows:
        tds = "".join(
            f'<td class="num">{cell(c)}</td>' if i in numeric else f"<td>{cell(c)}</td>"
            for i, c in enumerate(row)
        )
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def log_block(text: str) -> str:
    return f'<pre class="log">{esc(text.rstrip())}</pre>'


def code_block(title: str, path: Path, lexer) -> str:
    formatter = HtmlFormatter(linenos="inline", cssclass="codehilite")
    return (
        f'<section class="source"><h3>{esc(title)}</h3>'
        f"<p class='path'><code>{esc(str(path.relative_to(ROOT.parent)))}</code></p>"
        f"{highlight(path.read_text(), lexer, formatter)}</section>"
    )


def grab(text: str, pattern: str, default: str = "?") -> str:
    m = re.search(pattern, text, flags=re.M)
    return m.group(1) if m else default


def query_sections(text: str) -> list[tuple[str, str]]:
    """Split 04_queries.txt into (title, body) per '======== Qn: ... ========' header."""
    parts = re.split(r"^======== (Q\d: .*?) ========$", text, flags=re.M)
    out = []
    for i in range(1, len(parts), 2):
        out.append((parts[i], parts[i + 1].strip("\n")))
    return out


def main() -> None:
    css = HtmlFormatter(style="friendly").get_style_defs(".codehilite")
    create = (OUT / "01_create.txt").read_text()
    load = (OUT / "02_load.txt").read_text()
    describe = (OUT / "03_describe.txt").read_text()
    queries = (OUT / "04_queries.txt").read_text()

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
    timing_rows = []
    for title, body in qsecs:
        m = re.search(r"^\((.*?), ([\d.]+) ms\)$", body, flags=re.M)
        timing_rows.append([title, m.group(1) if m else "?", (m.group(2) + " ms") if m else "?"])

    design_rows = [
        ["customer profile", "<code>CUST#&lt;custkey&gt;</code>", "<code>PROFILE</code>",
         "type, custkey, name, address, nationkey, phone, acctbal, mktsegment, comment"],
        ["order with its line items", "<code>CUST#&lt;custkey&gt;</code>",
         "<code>ORDER#&lt;orderdate&gt;#&lt;orderkey, 10 digits&gt;</code>",
         "type, orderkey, custkey, orderstatus, totalprice, orderdate, year, orderpriority, clerk, shippriority, comment, "
         "<code>lineitems</code> = list of maps {linenumber, partkey, suppkey, quantity, extendedprice, discount, tax, "
         "returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment}"],
    ]
    access_rows = [
        ["1", "Retrieve a customer's profile", "<code>GetItem</code> (CUST#c, PROFILE)", "1"],
        ["2", "Retrieve all orders for a customer", "<code>Query</code> pk = CUST#c, sk <code>begins_with</code> ORDER#", "1 (paged)"],
        ["3", "Retrieve all line items of an order", "<code>Query</code> GSI OrderIndex orderkey = o → (pk, sk); <code>GetItem</code> the order; line items are inside it", "2"],
        ["4", "Customer's order history", "<code>GetItem</code> profile + <code>Query</code> pk = CUST#c, sk begins_with ORDER# (every order already carries its line items)", "2"],
        ["5", "Items ordered by a customer in a year Y", "<code>Query</code> pk = CUST#c, sk begins_with ORDER#Y; collect the embedded line items", "1"],
        ["6", "Items ordered in Y-1 but not in Y", "<code>Query</code> for ORDER#(Y-1) and <code>Query</code> for ORDER#Y; set difference of partkeys in the program", "2"],
        ["7", "Total value of a customer's orders in year Y", "<code>Query</code> pk = CUST#c, sk begins_with ORDER#Y; sum O_TOTALPRICE", "1"],
    ]

    outputs = [
        "<h3>6.1 Create table (<code>output/01_create.txt</code>)</h3>", log_block(create),
        "<h3>6.2 Load (<code>output/02_load.txt</code>)</h3>", log_block(load),
        "<h3>6.3 describe-table after the load (<code>output/03_describe.txt</code>)</h3>", log_block(describe),
    ]
    for n, (title, body) in enumerate(qsecs, start=4):
        outputs += [f"<h3>6.{n} {esc(title)}</h3>", log_block(body)]

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>IE-494 Lab 10 — Working with DynamoDB</title>
<style>
@page {{ size: A4; margin: 15mm 13mm 16mm 13mm; }}
* {{ box-sizing: border-box; }}
body {{ font-family: "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif; font-size: 10.2pt; line-height: 1.45; color: #1a1a1a; }}
h1 {{ font-size: 20pt; margin: 0 0 6px; }}
h2 {{ font-size: 14pt; border-bottom: 1.5px solid #1f4e79; padding-bottom: 3px; margin-top: 22px; color: #1f4e79; page-break-after: avoid; }}
h3 {{ font-size: 11.5pt; margin-top: 16px; color: #244e6b; page-break-after: avoid; }}
.meta {{ background: #eef4fb; border-left: 4px solid #1f4e79; padding: 10px 14px; margin: 10px 0 18px; }}
.meta p {{ margin: 2px 0; }}
table {{ border-collapse: collapse; width: 100%; margin: 8px 0 14px; font-size: 9pt; }}
th, td {{ border: 1px solid #c5d0dc; padding: 3px 6px; text-align: left; vertical-align: top; }}
td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
th {{ background: #1f4e79; color: white; }}
tr:nth-child(even) td {{ background: #f6f9fc; }}
code, pre {{ font-family: "SF Mono", Menlo, Consolas, monospace; }}
code {{ font-size: 8.6pt; }}
pre.log {{ font-size: 6.9pt; line-height: 1.28; background: #f7f8fa; border: 1px solid #dde3ea; border-radius: 4px; padding: 7px 9px; white-space: pre-wrap; word-break: break-all; }}
pre.key {{ font-size: 8.4pt; line-height: 1.35; background: #f7f8fa; border: 1px solid #dde3ea; border-radius: 4px; padding: 7px 9px; }}
.path {{ color: #555; margin: 0 0 6px; font-size: 9pt; }}
.codehilite {{ border: 1px solid #d9e2ec; border-radius: 4px; margin: 0 0 16px; font-size: 7.6pt; }}
.codehilite pre {{ margin: 0; padding: 6px 8px; white-space: pre-wrap; line-height: 1.35; }}
.codehilite .linenos {{ color: #8a93a0; background: #eef1f5; padding: 0 4px; margin-right: 8px; }}
.note {{ font-size: 9.3pt; color: #333; }}
ul, ol {{ margin-top: 4px; }}
li {{ margin: 2px 0; }}
{css}
</style>
</head>
<body>
<h1>IE-494 Lab 10 — Working with DynamoDB</h1>
<div class="meta">
<p><strong>Course:</strong> IE-494 Big Data Processing · Autumn 2026</p>
<p><strong>Instructor:</strong> PM Jat (<code>pm_jat@dau.ac.in</code>)</p>
<p><strong>Student:</strong> Aditya Vaish · ID <code>202401401</code></p>
<p><strong>Submitted:</strong> 2 November 2026</p>
<p><strong>Stack:</strong> DynamoDB Local 3.3.2, AWS SDK for Java 2.31.0 (<code>dynamodb</code>, <code>dynamodb-enhanced</code>), Java (release 17), Maven, macOS arm64</p>
</div>

<h2>1. Assignment</h2>
<p>Take <code>CUSTOMER</code>, <code>ORDERS</code> and <code>LINEITEM</code> from the TPC-H dataset (scale factor 1, generated with
<code>dbgen</code> as in Lab 08: {customers} customers, {orders} orders, {lines} line items), design a DynamoDB database for the query load
below, populate it, and answer every query in a Java program with one function per query, using the high-level (Enhanced Client) API.</p>
<ol>
<li>Retrieve a customer's profile.</li>
<li>Retrieve all orders for a customer.</li>
<li>Retrieve all line items belonging to an order.</li>
<li>Retrieve the details required to display a customer's order history.</li>
<li>List all items ordered by a customer in a year.</li>
<li>List all items a customer ordered in the previous year (year-1) but has not bought this year.</li>
<li>Show the total value of a customer's orders in a given year.</li>
</ol>

<h2>2. Database design</h2>
<p>Every query is driven by one customer (1, 2, 4, 5, 6, 7) or one order (3), so the data is modelled as
<strong>aggregates</strong> in a <strong>single table <code>TPCH</code></strong> with a composite primary key
<code>pk</code> (partition, string) and <code>sk</code> (sort, string):</p>
<ul>
<li>A customer's partition <code>CUST#&lt;custkey&gt;</code> holds the profile item (<code>sk = PROFILE</code>) and one item per order.</li>
<li>An order item's sort key <code>ORDER#&lt;orderdate&gt;#&lt;orderkey&gt;</code> starts with the date, so the orders of a customer
are stored in date order and "orders of year Y" is just <code>begins_with(sk, "ORDER#Y")</code>. The 10-digit zero-padded order key at the end keeps the key unique.</li>
<li>The line items of an order are <strong>embedded</strong> in the order item as a list of maps (the <code>CORDERS</code> aggregate of Lab 08).
TPC-H orders have 1–7 line items, so an item is at most a few KB, far below the 400 KB limit, and every order-level query gets the line items for free.</li>
<li>Query 3 is addressed by order key only, so a <strong>global secondary index <code>OrderIndex</code></strong> with partition key <code>orderkey</code>
(projection <code>KEYS_ONLY</code>) maps an order key to the order's (pk, sk); a <code>GetItem</code> then returns the order with its line items.
KEYS_ONLY keeps the index small: it stores only pk, sk and orderkey per order.</li>
<li>No <code>Scan</code> is needed for any of the seven queries; every query is a key lookup or a single-partition <code>Query</code>.</li>
</ul>
{table(["item", "pk", "sk", "other attributes"], design_rows, raw=True)}
<p class="note">Items as stored in DynamoDB:</p>
<pre class="key">{{ "pk": "CUST#121", "sk": "PROFILE", "type": "CUSTOMER", "custkey": 121, "name": "Customer#000000121", "address": "...",
  "nationkey": 17, "phone": "27-411-990-2959", "acctbal": 6428.32, "mktsegment": "BUILDING", "comment": "..." }}

{{ "pk": "CUST#121", "sk": "ORDER#1994-06-26#0005512869", "type": "ORDER", "orderkey": 5512869, "custkey": 121,
  "orderstatus": "F", "totalprice": 133146.02, "orderdate": "1994-06-26", "year": 1994, "orderpriority": "...", "clerk": "Clerk#...",
  "shippriority": 0, "comment": "...",
  "lineitems": [ {{ "linenumber": 1, "partkey": ..., "suppkey": ..., "quantity": ..., "extendedprice": ..., "discount": ..., "tax": ...,
                   "returnflag": "...", "linestatus": "...", "shipdate": "...", "commitdate": "...", "receiptdate": "...",
                   "shipinstruct": "...", "shipmode": "...", "comment": "..." }}, ... ] }}</pre>
<h3>Query load → DynamoDB operations</h3>
{table(["#", "query", "operation", "requests"], access_rows, raw=True)}

<h2>3. Setup</h2>
<ul>
<li><strong>DynamoDB Local</strong> (<code>dynamodb_local_latest</code>, version 3.3.2) started as in the setup sheet:
<code>java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -port 8000 -dbPath lab10/db</code>.
The program connects with endpoint <code>http://localhost:8000</code>, region <code>ap-south-1</code>, access key / secret <code>1234</code> / <code>1234</code>.</li>
<li><strong>Data:</strong> <code>customer.tbl</code>, <code>orders.tbl</code>, <code>lineitem.tbl</code> from <code>dbgen -s 1</code> (the 1 GB TPC-H population used in Labs 08 and 09).</li>
<li><strong>Dependencies</strong> (from the lab sheet): AWS SDK BOM 2.31.0, <code>dynamodb</code>, <code>dynamodb-enhanced</code>, <code>slf4j-simple</code> 1.7.36.</li>
<li><strong>Run:</strong> <code>bash lab10/run_all.sh</code> → create table, load, describe, then the seven queries for customer 121, its order 5512869 and year 1994.
Customer 121 was chosen because it has 15 orders with orders in two consecutive years (3 in 1993, 5 in 1994), which exercises queries 5–7.</li>
</ul>

<h2>4. How it works</h2>
<ul>
<li><strong>Model classes</strong> (<code>Customer</code>, <code>Order</code>, <code>LineItem</code>): <code>@DynamoDbBean</code> POJOs.
<code>pk</code>/<code>sk</code> carry <code>@DynamoDbPartitionKey</code>/<code>@DynamoDbSortKey</code>; <code>Order.orderkey</code> carries
<code>@DynamoDbSecondaryPartitionKey(indexNames = "OrderIndex")</code>; <code>Order.lineitems</code> is a <code>List&lt;LineItem&gt;</code>,
which the Enhanced Client maps to a DynamoDB list of maps. Money columns are <code>BigDecimal</code> (DynamoDB numbers).</li>
<li><strong>Create</strong> (<code>Schema.create</code>): <code>DynamoDbTable&lt;Order&gt;.createTable(...)</code> with an <code>EnhancedGlobalSecondaryIndex</code>
for <code>OrderIndex</code>; the key schema and attribute definitions come from the bean annotations. On-demand (PAY_PER_REQUEST) billing.</li>
<li><strong>Load</strong> (<code>TpchLoader</code>): <code>customer.tbl</code> rows become profile items. <code>orders.tbl</code> and <code>lineitem.tbl</code>
are both written by dbgen in ascending order key, so they are merge-joined in one streaming pass: read an order, take its line items from the head of
<code>lineitem.tbl</code>, put the list into the order bean. Items go out as <code>BatchWriteItem</code> requests of 25 (<code>DynamoDbEnhancedClient.batchWriteItem</code>
with a <code>WriteBatch</code> per request), {esc(threads)} writer threads, unprocessed items re-sent with backoff.</li>
<li><strong>Queries</strong> (<code>Queries</code>, one method each): <code>DynamoDbTable.getItem(Key)</code>, <code>DynamoDbTable.query(QueryConditional.sortBeginsWith(...))</code>
and <code>DynamoDbIndex.query(QueryConditional.keyEqualTo(...))</code>. Query results are iterated page by page; the program counts the pages and the elapsed time.
Query 6 is two Queries plus a set difference of part keys in Java; Query 7 sums <code>totalprice</code> and cross-checks it against
Σ extendedprice × (1 − discount) × (1 + tax) of the embedded line items.</li>
</ul>

<h2>5. Results</h2>
{table(
    ["", "value"],
    [
        ["customer.tbl rows → profile items", customers],
        ["orders.tbl rows → order items", orders],
        ["lineitem.tbl rows (embedded in the orders)", lines],
        ["profile items put", prof],
        ["order items put", ords],
        ["items in the table", total_items],
        ["batch retries", retries],
        ["describe-table item count / size", f"{item_count} / {table_size}"],
    ],
    numeric={1},
)}
<p class="note">Requests and wall time of each query (customer 121, order 5512869, year 1994), from <code>output/04_queries.txt</code>:</p>
{table(["query", "requests", "time"], timing_rows, numeric={2})}
<ul>
<li><strong>Q1</strong> is a single key lookup. <strong>Q2, Q5, Q7</strong> are one single-partition Query each, and the year queries read only the
orders of that year because the year is a prefix of the sort key. <strong>Q4</strong> needs one extra GetItem for the profile.</li>
<li><strong>Q3</strong> costs one index Query plus one GetItem, the price of a KEYS_ONLY projection; projecting ALL would make it one request at the cost of
duplicating every order (with its line items) into the index.</li>
<li><strong>Q6</strong> cannot be a single DynamoDB request (set difference is not a key condition); it is two year Queries and a <code>TreeSet</code> difference in the client.
TPC-H part keys are random over 200,000 parts, so the two years share no part and the answer is every part of year-1.</li>
<li><strong>Q7:</strong> the sum of <code>totalprice</code> (498,215.67) and the value recomputed from the embedded line items (498,215.88) differ by 21 cents because dbgen truncates each line's extendedprice × (1 − discount) × (1 + tax) to whole cents before adding it into O_TOTALPRICE; both numbers come from the same single Query.</li>
<li>Load throughput is bounded by DynamoDB Local (one SQLite database behind the HTTP endpoint), not by the client; profile items are small and
order items carry 1–7 line items each.</li>
</ul>

<h2>6. Program outputs</h2>
{''.join(outputs)}

<h2>7. Source code</h2>
<p>All of <code>lab10/</code>: line-numbered and syntax-highlighted Java, XML and bash.</p>
{''.join(code_block(t, p, lx) for t, p, lx in SOURCES)}
</body>
</html>
"""
    HTML_PATH.write_text(html_doc)
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    subprocess.run(
        [chrome, "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
         f"--print-to-pdf={PDF}", HTML_PATH.as_uri()],
        check=True,
        capture_output=True,
    )
    print(f"wrote {HTML_PATH}")
    print(f"wrote {PDF} ({PDF.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
