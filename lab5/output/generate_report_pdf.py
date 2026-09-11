#!/usr/bin/env python3
"""Build a single submission PDF: evaluation results + full syntax-highlighted source."""

from __future__ import annotations

import html
import subprocess
from pathlib import Path

from pygments import highlight
from pygments.formatters import HtmlFormatter
from pygments.lexers import BashLexer, JavaLexer, XmlLexer, get_lexer_by_name

ROOT = Path("/Users/avaish/IE-494/lab5")
OUT = ROOT / "output"
PDF = OUT / "IE-494-Lab05-YAFIM-Report.pdf"
HTML_PATH = OUT / "IE-494-Lab05-YAFIM-Report.html"

SOURCES = [
    ("Yafim.java", ROOT / "src/main/java/pmj/bdp/spark/lab5/Yafim.java", JavaLexer()),
    ("HashTree.java", ROOT / "src/main/java/pmj/bdp/spark/lab5/HashTree.java", JavaLexer()),
    ("Itemset.java", ROOT / "src/main/java/pmj/bdp/spark/lab5/Itemset.java", JavaLexer()),
    ("SparkUtil.java", ROOT / "src/main/java/pmj/bdp/spark/lab5/SparkUtil.java", JavaLexer()),
    ("pom.xml", ROOT / "pom.xml", XmlLexer()),
    ("run_all.sh", ROOT / "run_all.sh", BashLexer()),
]


def load_tsv(path: Path) -> list[tuple[str, str]]:
    rows = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        key, count = line.split("\t", 1)
        rows.append((key, count))
    return rows


def table(headers: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for row in rows:
        tds = "".join(f"<td>{html.escape(c)}</td>" for c in row)
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def kv_table(pairs: list[tuple[str, str]], h1: str, h2: str) -> str:
    return table([h1, h2], [[a, b] for a, b in pairs])


def code_block(title: str, path: Path, lexer) -> str:
    src = path.read_text()
    formatter = HtmlFormatter(nowrap=False, linenos="table", cssclass="codehilite")
    highlighted = highlight(src, lexer, formatter)
    return (
        f'<section class="source">'
        f"<h3>{html.escape(title)}</h3>"
        f"<p class='path'><code>{html.escape(str(path.relative_to(ROOT.parent)))}</code></p>"
        f"{highlighted}</section>"
    )


def main() -> None:
    l1 = load_tsv(OUT / "yafim/L1.txt")
    l2 = load_tsv(OUT / "yafim/L2.txt")
    l3 = load_tsv(OUT / "yafim/L3.txt")
    l4 = load_tsv(OUT / "yafim/L4.txt")
    formatter = HtmlFormatter(style="friendly", linenos="table", cssclass="codehilite")
    css = formatter.get_style_defs(".codehilite")

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>IE-494 Lab 05 — YAFIM Spark RDD Report</title>
<style>
@page {{
  size: A4;
  margin: 16mm 14mm 18mm 14mm;
  @bottom-center {{
    content: "IE-494 Lab 05 · Aditya Vaish · 202401401 · page " counter(page);
    font-size: 9px;
    color: #555;
  }}
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: "SF Pro Text", "Helvetica Neue", Helvetica, Arial, sans-serif;
  font-size: 10.5pt;
  line-height: 1.45;
  color: #1a1a1a;
}}
h1 {{ font-size: 20pt; margin: 0 0 6px; }}
h2 {{ font-size: 14pt; border-bottom: 1.5px solid #1f4e79; padding-bottom: 3px; margin-top: 22px; color: #1f4e79; page-break-after: avoid; }}
h3 {{ font-size: 12pt; margin-top: 16px; color: #244e6b; page-break-after: avoid; }}
.meta {{ background: #eef4fb; border-left: 4px solid #1f4e79; padding: 10px 14px; margin: 10px 0 18px; }}
.meta p {{ margin: 2px 0; }}
table {{ border-collapse: collapse; width: 100%; margin: 8px 0 14px; font-size: 9.5pt; page-break-inside: auto; }}
th, td {{ border: 1px solid #c5d0dc; padding: 4px 7px; text-align: left; }}
th {{ background: #1f4e79; color: white; }}
tr:nth-child(even) td {{ background: #f6f9fc; }}
code, pre {{ font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 8.4pt; }}
pre {{ white-space: pre-wrap; }}
.path {{ color: #555; margin: 0 0 6px; font-size: 9pt; }}
.source {{ page-break-inside: auto; }}
.source h3 {{ page-break-after: avoid; }}
.codehilite {{ border: 1px solid #d9e2ec; border-radius: 4px; overflow: hidden; margin: 0 0 18px; }}
.codehilite table {{ margin: 0; width: 100%; border: 0; }}
.codehilite td {{ border: 0; padding: 0 8px; vertical-align: top; }}
.codehilite .linenos {{ width: 2.6em; color: #8a93a0; background: #f3f5f8; text-align: right; user-select: none; }}
.codehilite .linenos pre {{ margin: 8px 0; }}
.codehilite .code pre {{ margin: 8px 0; }}
.note {{ font-size: 9.5pt; color: #333; }}
ul {{ margin-top: 4px; }}
{css}
</style>
</head>
<body>
<h1>IE-494 Lab 05 — YAFIM (Spark RDD)</h1>
<div class="meta">
<p><strong>Course:</strong> IE-494 Big Data Processing · Autumn 2026</p>
<p><strong>Instructor:</strong> PM Jat (<code>pm_jat@dau.ac.in</code>)</p>
<p><strong>Student:</strong> Aditya Vaish · ID <code>202401401</code></p>
<p><strong>Submitted:</strong> 11 September 2026</p>
<p><strong>Constraint:</strong> Spark <strong>RDD API only</strong>. No DataFrames, Datasets, or Spark SQL.</p>
</div>

<h2>1. Assignment</h2>
<p>Implement <strong>basic YAFIM</strong> (Yet Another Frequent Itemset Mining) on Spark RDDs:</p>
<ul>
<li>Paper Algorithm 2 — frequent 1-itemsets</li>
<li>Paper Algorithm 3 — frequent k-itemsets for k ≥ 2 (apriori-gen + broadcast hash tree + <code>subset()</code>)</li>
<li>Mine FIMI <code>retail.dat</code> with <code>minSupport = 1000</code></li>
</ul>
<p><strong>Citation:</strong> Hongjian Qiu, Rong Gu, Chunfeng Yuan, Yihua Huang,
<em>YAFIM: A Parallel Frequent Itemset Mining Algorithm with Spark</em>,
IEEE International Parallel &amp; Distributed Processing Symposium Workshops (IPDPSW 2014).</p>

<h2>2. Dataset</h2>
{table(
    ["property", "value"],
    [
        ["transactions (baskets)", "88,162"],
        ["encoding", "space-separated item IDs, one basket per line"],
        ["minSupport", "1000"],
        ["relative support", "1000 / 88162 ≈ 1.13%"],
        ["Spark master", "local[*] (Spark 4.2.0, spark-core_2.13, Java 17)"],
        ["API", "Java RDD only — no DataFrame / Dataset / Spark SQL"],
    ],
)}
<p>A tiny <code>data/toy.dat</code> (5 baskets) is used as a sanity check with <code>minSupport = 3</code>.</p>

<h2>3. Algorithm</h2>
<p>YAFIM is Apriori on Spark RDDs. The transaction RDD is parsed once, cached, and reused for every pass.</p>
<h3>Phase I — Algorithm 2 (frequent 1-itemsets)</h3>
<ol>
<li><code>flatMap</code> each cached transaction to <code>(item, 1)</code> pairs.</li>
<li><code>reduceByKey</code> to support counts.</li>
<li>Keep items with count ≥ <code>minSupport</code>. That list is <code>L1</code>.</li>
</ol>
<h3>Phase II — Algorithm 3 (k ≥ 2)</h3>
<p>For k = 2, 3, … until <code>Ck</code> or <code>Lk</code> is empty:</p>
<ol>
<li><strong>apriori-gen:</strong> self-join frequent (k−1)-itemsets on a shared (k−2)-prefix, then prune any k-candidate whose (k−1)-subsets are not all frequent.</li>
<li>Build an in-memory <strong>hash tree</strong> of <code>Ck</code> (branch factor 11, leaf split after 4 candidates) and <strong>broadcast</strong> it.</li>
<li>Each partition runs <code>subset()</code> / <code>HashTree.findCandidates</code> on every cached transaction: walk the tree by hashing successive items, and at leaves test <code>containsAll</code>.</li>
<li><code>flatMap</code> matching candidates to <code>(key, 1)</code>, <code>reduceByKey</code>, keep counts ≥ <code>minSupport</code> → <code>Lk</code>.</li>
</ol>
<p><code>SparkContext.collect()</code> returns an unmodifiable list, so results are copied into <code>new ArrayList&lt;&gt;(...)</code> before <code>List.sort</code>.</p>
<h3>Hash-tree support counting (bug fix)</h3>
<p>Hashing two different items into the same bucket can visit a leaf more than once for the same transaction.
Without dedupe, the same candidate was emitted multiple times per basket, which inflated support.
Example before the fix: pair <code>39,48</code> counted <strong>75856</strong> (YAFIM) vs <strong>29142</strong> (lab3 MR-Apriori), and <code>|L2|</code> was <strong>227</strong> instead of <strong>49</strong>.</p>
<p><code>findCandidates</code> now records keys in a <code>LinkedHashSet</code> and returns each candidate once per transaction.
After the fix, YAFIM matches lab3 on every itemset key and count.</p>

<h2>4. How to run</h2>
<pre><code>cd lab5
bash run_all.sh

# equivalent:
mvn -DskipTests package
mvn exec:java \\
  -Dexec.mainClass=pmj.bdp.spark.lab5.Yafim \\
  -Dexec.args="$(pwd)/data/retail.dat 1000 $(pwd)/output/yafim"
</code></pre>
<p class="note"><code>mvn exec:java</code> does not recompile unless you also <code>package</code> / <code>compile</code>.
Outputs: <code>output/yafim/L1.txt</code> … <code>L4.txt</code> and <code>frequent-itemsets.txt</code> (tab-separated <code>itemset\\tsupport</code>).</p>

<h2>5. Evaluation results</h2>
<h3>5.1 Toy sanity check (<code>toy.dat</code>, minSupport = 3)</h3>
<p>5 transactions. Expected: L1 = items 1, 2, 3; three frequent pairs; L3 empty (the triple 1,2,3 has support 2).</p>
{kv_table([("1","4"),("2","4"),("3","4"),("1,2","3"),("1,3","3"),("2,3","3")], "itemset", "support")}
<p>L1 = 3, C2 = 3, L2 = 3, C3 = 1, L3 = 0. Matches the hand count.</p>

<h3>5.2 Retail (<code>retail.dat</code>, minSupport = 1000)</h3>
<p><strong>transactions = 88162</strong>, relative minsup ≈ <strong>1.13%</strong>.</p>
{table(
    ["level", "YAFIM Lk count", "lab3 MR-Apriori Lk count", "YAFIM Ck count", "match?"],
    [
        ["L1", "56", "56", "— (Algo-2)", "yes"],
        ["L2", "49", "49", "1540", "yes"],
        ["L3", "24", "24", "31", "yes"],
        ["L4", "6", "6", "6", "yes"],
        ["L5", "0 (stop)", "0", "0", "yes"],
    ],
)}
<p>Total frequent itemsets written: <strong>135</strong> (56 + 49 + 24 + 6). Lab3’s combined
<code>frequent-itemsets.txt</code> lists k ≥ 2 only (79 itemsets). Every k ≥ 2 key and support in YAFIM
equals lab3 (0 missing keys, 0 count mismatches).</p>
{table(
    ["source", "support of 39,48"],
    [
        ["YAFIM after HashTree dedupe", "29142"],
        ["lab3 MR-Apriori", "29142"],
        ["YAFIM before dedupe (buggy)", "75856"],
    ],
)}
<p>Singleton supports also match (<code>39</code> → 50675, <code>48</code> → 42135).</p>

<h3>5.3 L4 — all 6 frequent 4-itemsets</h3>
{kv_table(l4, "itemset", "support")}
<h3>5.4 L3 — all 24 frequent 3-itemsets</h3>
{kv_table(l3, "itemset", "support")}
<h3>5.5 L2 — all 49 frequent 2-itemsets</h3>
{kv_table(l2, "itemset", "support")}
<h3>5.6 L1 — all 56 frequent 1-itemsets</h3>
{kv_table(l1, "item", "support")}

<h3>5.7 Terminal summary (retail)</h3>
<pre><code>transactions = 88162
minSupport   = 1000  (0.0113 of n)

======== YAFIM Algo-2  L1 ========
|L1| = 56

======== YAFIM Algo-3  L2 ========
|C2| = 1540
|L2| = 49

======== YAFIM Algo-3  L3 ========
|C3| = 31
|L3| = 24

======== YAFIM Algo-3  L4 ========
|C4| = 6
|L4| = 6

======== YAFIM Algo-3  L5 ========
|C5| = 0

Wrote 135 frequent itemsets to output/yafim/frequent-itemsets.txt

56  output/yafim/L1.txt
49  output/yafim/L2.txt
24  output/yafim/L3.txt
 6  output/yafim/L4.txt
135 output/yafim/frequent-itemsets.txt
</code></pre>

<h2>6. Source code (complete, formatted)</h2>
<p>All of <code>lab5/</code>. Spark RDD only. Line-numbered, syntax-highlighted Java / XML / bash.</p>
{''.join(code_block(title, path, lexer) for title, path, lexer in SOURCES)}
</body>
</html>
"""
    HTML_PATH.write_text(html_doc)
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    cmd = [
        chrome,
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={PDF}",
        HTML_PATH.as_uri(),
    ]
    subprocess.run(cmd, check=True)
    print(f"wrote {HTML_PATH}")
    print(f"wrote {PDF} ({PDF.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
