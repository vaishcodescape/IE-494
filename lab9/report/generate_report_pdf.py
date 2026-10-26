#!/usr/bin/env python3
"""Build the single submission PDF: approach, results, all program outputs, full highlighted source."""

from __future__ import annotations

import csv
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
PDF = REPORT / "IE-494-Lab09-RocksDB-Compaction-Report.pdf"
HTML_PATH = REPORT / "IE-494-Lab09-RocksDB-Compaction-Report.html"
JAVA = ROOT / "src/main/java/pmj/bdp/rocks/lab9"

SOURCES = [
    ("Lab09.java", JAVA / "Lab09.java", JavaLexer()),
    ("CordersJson.java", JAVA / "CordersJson.java", JavaLexer()),
    ("CompactionStudy.java", JAVA / "CompactionStudy.java", JavaLexer()),
    ("SstEventListener.java", JAVA / "SstEventListener.java", JavaLexer()),
    ("SstTables.java", JAVA / "SstTables.java", JavaLexer()),
    ("SstIndex.java", JAVA / "SstIndex.java", JavaLexer()),
    ("pom.xml", ROOT / "pom.xml", XmlLexer()),
    ("run_all.sh", ROOT / "run_all.sh", BashLexer()),
]

STYLES = ["leveled", "universal"]
EVENT = re.compile(r"^\[event (\d+)\s+t=\s*([\d.]+)s\] (.*)$")


def esc(s: str) -> str:
    return html.escape(s)


def table(headers: list[str], rows: list[list[str]], numeric: set[int] | None = None) -> str:
    numeric = numeric or set()
    th = "".join(f"<th>{esc(h)}</th>" for h in headers)
    body = []
    for row in rows:
        tds = "".join(
            f'<td class="num">{esc(c)}</td>' if i in numeric else f"<td>{esc(c)}</td>"
            for i, c in enumerate(row)
        )
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(body)}</tbody></table>"


def log_block(text: str) -> str:
    return f'<pre class="log">{esc(text.rstrip())}</pre>'


def mib(n: int) -> str:
    return f"{n / (1024 * 1024):,.1f} MiB"


def summary_rows() -> dict[str, dict[str, str]]:
    with (OUT / "summary.tsv").open() as f:
        return {row["style"]: row for row in csv.DictReader(f, delimiter="\t")}


def compactions(style: str) -> list[list[str]]:
    """One row per compaction, from the BEGIN/DONE headers in events.txt."""
    begins: dict[str, tuple[str, str]] = {}
    rows = []
    for line in (OUT / style / "events.txt").read_text().splitlines():
        m = EVENT.match(line)
        if not m:
            continue
        _, t, text = m.groups()
        b = re.match(r"COMPACTION BEGIN\s+job=(\d+) reason=(\S+) (L\d+ -> L\d+)", text)
        if b:
            begins[b.group(1)] = (b.group(2), t)
            continue
        d = re.match(
            r"COMPACTION DONE\s+job=(\d+) compaction #(\d+) (L\d+ -> L\d+) (TRIVIAL MOVE|merge).*?: "
            r"(\d+) in, (\d+) out, read ([\d,]+), wrote ([\d,]+) bytes",
            text,
        )
        if d:
            job, n, levels, kind, n_in, n_out, read, wrote = d.groups()
            reason, started = begins.get(job, ("?", t))
            secs = float(t) - float(started)
            rows.append([n, reason, levels, "trivial move" if kind.startswith("TRIVIAL") else "merge",
                         n_in, n_out, read, wrote, f"{secs:.3f}"])
    return rows


def flush_reasons(style: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for reason in re.findall(r"FLUSH BEGIN\s+job=\d+ reason=(\S+)", (OUT / style / "events.txt").read_text()):
        counts[reason] = counts.get(reason, 0) + 1
    return counts


def max_l0(style: str) -> int:
    return max(int(n) for n in re.findall(r"\bL0=(\d+)", (OUT / style / "events.txt").read_text()))


def shortened_separators(style: str, limit: int = 2) -> list[str]:
    """Index separator keys that RocksDB shortened below the 10-digit key width."""
    found = []
    for m in re.finditer(r"^\s+\d+  (\d+)\s+[\d,]+\s+[\d,]+$", (OUT / style / "index_dump.txt").read_text(), re.M):
        key = m.group(1)
        if len(key) < 10 and key not in found:
            found.append(key)
            if len(found) == limit:
                break
    return found


def run_log_without_final_tables() -> str:
    """run_log.txt minus the final-SSTable sections, which are printed in full from their own files."""
    keep = []
    skipping = False
    for line in (OUT / "run_log.txt").read_text().splitlines():
        if line.startswith("========"):
            skipping = "final SSTables" in line
            if skipping:
                keep.append(line + "\n(final SSTables: see the final_sstables.txt sections below)")
                continue
        if not skipping:
            keep.append(line)
    return "\n".join(keep)


def head(path: Path, lines: int) -> str:
    out = []
    with path.open() as f:
        for i, line in enumerate(f):
            if i >= lines:
                break
            out.append(line.rstrip("\n"))
    return "\n".join(out)


def line_count(path: Path) -> int:
    with path.open() as f:
        return sum(1 for _ in f)


def code_block(title: str, path: Path, lexer) -> str:
    # Inline numbers stay on their own line when a long line wraps; a table column drifts.
    formatter = HtmlFormatter(linenos="inline", cssclass="codehilite")
    return (
        f'<section class="source"><h3>{esc(title)}</h3>'
        f"<p class='path'><code>{esc(str(path.relative_to(ROOT.parent)))}</code></p>"
        f"{highlight(path.read_text(), lexer, formatter)}</section>"
    )


def main() -> None:
    s = summary_rows()
    lv, un = s["leveled"], s["universal"]
    css = HtmlFormatter(style="friendly").get_style_defs(".codehilite")
    comp = {style: compactions(style) for style in STYLES}
    dumps = {style: OUT / style / "index_dump.txt" for style in STYLES}
    uni_inputs = " → ".join(r[4] for r in comp["universal"])
    reasons = flush_reasons("leveled")
    full = reasons.get("WRITE_BUFFER_FULL", 0)
    manual = reasons.get("MANUAL_FLUSH", 0)
    peak = {style: max_l0(style) for style in STYLES}
    separators = " or ".join(f"<code>{k}</code>" for k in shortened_separators("leveled"))

    summary = table(
        ["", "leveled", "universal"],
        [
            ["orders loaded", f"{int(lv['orders']):,}", f"{int(un['orders']):,}"],
            ["flushes", lv["flushes"], un["flushes"]],
            ["compactions", lv["compactions"], un["compactions"]],
            ["  trivial moves / merges", f"{lv['trivial_moves']} / {lv['merges']}", f"{un['trivial_moves']} / {un['merges']}"],
            ["bytes flushed to L0", f"{int(lv['flushed_bytes']):,}", f"{int(un['flushed_bytes']):,}"],
            ["bytes rewritten by compaction", f"{int(lv['compaction_written_bytes']):,}", f"{int(un['compaction_written_bytes']):,}"],
            ["write amplification", lv["write_amp"], un["write_amp"]],
            ["final SSTables", lv["final_files"], un["final_files"]],
            ["final layout", lv["final_levels"], un["final_levels"]],
            ["final SST bytes", f"{int(lv['final_sst_bytes']):,}", f"{int(un['final_sst_bytes']):,}"],
            ["load time (writes)", f"{float(lv['load_s']):.2f} s", f"{float(un['load_s']):.2f} s"],
            ["last flush + compactions settle", f"{float(lv['settle_s']):.2f} s", f"{float(un['settle_s']):.2f} s"],
        ],
        numeric={1, 2},
    )
    comp_headers = ["#", "reason", "levels", "kind", "in", "out", "bytes read", "bytes written", "secs"]

    outputs = [
        "<h3>6.1 Console log (dump, load progress, comparison)</h3>",
        "<p class='note'><code>output/run_log.txt</code>.</p>",
        log_block(run_log_without_final_tables()),
    ]
    n = 2
    for style in STYLES:
        title = style.capitalize()
        outputs += [
            f"<h3>6.{n} {title}: every flush and compaction (<code>output/{style}/events.txt</code>)</h3>",
            log_block((OUT / style / "events.txt").read_text()),
            f"<h3>6.{n + 1} {title}: final SSTables, table properties and index excerpt "
            f"(<code>output/{style}/final_sstables.txt</code>)</h3>",
            log_block((OUT / style / "final_sstables.txt").read_text()),
        ]
        n += 2
    outputs += [
        f"<h3>6.{n} Full index dumps (<code>index_dump.txt</code>)</h3>",
        "<p>The complete index of every final SSTable is written to "
        + " and ".join(
            f"<code>output/{st}/index_dump.txt</code> ({line_count(dumps[st]):,} lines, "
            f"{dumps[st].stat().st_size / 1e6:.1f} MB)" for st in STYLES
        )
        + ". They are too long to print in full, so here is the start of each file. "
        "Every SSTable section has the same layout and lists every index entry.</p>",
    ]
    for style in STYLES:
        outputs.append(f"<p class='note'><strong>{style}</strong>, first 40 lines:</p>")
        outputs.append(log_block(head(dumps[style], 40)))

    html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>IE-494 Lab 09 — Compaction in RocksDB</title>
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
<h1>IE-494 Lab 09 — Study of Compaction in RocksDB</h1>
<div class="meta">
<p><strong>Course:</strong> IE-494 Big Data Processing · Autumn 2026</p>
<p><strong>Instructor:</strong> PM Jat (<code>pm_jat@dau.ac.in</code>)</p>
<p><strong>Student:</strong> Aditya Vaish · ID <code>202401401</code></p>
<p><strong>Submitted:</strong> 26 October 2026</p>
<p><strong>Stack:</strong> RocksJava <code>rocksdbjni 10.10.1.1</code>, Java (release 17), Maven, macOS arm64 (8 cores, 8 GB)</p>
</div>

<h2>1. Assignment</h2>
<ol>
<li>Dump Lab 08's <code>CORDERS</code> (each order with its line items embedded) to <code>corders.json</code>.</li>
<li>Iterate through every order in that file and add it to a new RocksDB database <code>CORDERS</code>. Catch every
flush and compaction, and for each one print the SSTables before and after it: file name, level, size, smallest key, largest key.</li>
<li>After all orders are added, print the final SSTables the same way and also dump the index of each SSTable.</li>
<li>Repeat steps 2 and 3 for <strong>leveled</strong> and <strong>universal</strong> compaction.</li>
</ol>

<h2>2. Data and settings</h2>
<p><strong>Input.</strong> TPC-H scale factor 1, built with Lab 08's code: 1,500,000 orders and 6,001,215 line items
merged into <code>lab8/db/CORDERS_zstd</code>. <code>corders.json</code> is one JSON array with one order per line,
2,583,603,406 bytes, key range <code>0000000001 .. 0006000000</code>. Keys are Lab 08's 10-digit zero-padded
<code>O_ORDERKEY</code>, so byte order matches numeric order.</p>
{table(
    ["option", "value", "why"],
    [
        ["write_buffer_size × max_write_buffer_number", "128 MiB × 2", "about 20 flushes for 2.6 GB, so the event log stays readable"],
        ["level0_file_num_compaction_trigger", "4", "default"],
        ["target_file_size_base / max_bytes_for_level_base", "64 MiB / 256 MiB", "default 64 MiB target; 256 MiB level base (same as Lab 08)"],
        ["level_compaction_dynamic_level_bytes (leveled)", "true", "default since RocksDB 8.4"],
        ["universal options", "size_ratio=1, min_merge_width=2, max_size_amplification_percent=200", "defaults, set explicitly"],
        ["compression", "LZ4 (every level)", "fast; one codec for all levels"],
        ["block_size", "64 KiB", "about 37 orders per data block, so each index has about 2,100 entries per file instead of about 40,000"],
        ["format_version / index compression", "5 / off", "fixed on-disk layout for the index parser"],
        ["max_background_jobs", "2", "one flush thread and one compaction thread, so the before/after snapshots are easy to follow"],
        ["writes", "WriteBatch of 2,000, WAL off", "same as Lab 08"],
    ],
)}

<h2>3. How it works</h2>
<ul>
<li><strong>Dump</strong> (<code>CordersJson.dump</code>): iterate Lab 08's CORDERS with a read-only <code>RocksIterator</code> and stream each value to <code>corders.json</code>.</li>
<li><strong>Load</strong> (<code>CompactionStudy.run</code>): read <code>corders.json</code> line by line, take the key from the
<code>O_ORDERKEY</code> field, and <code>put</code> the order document into a fresh <code>CORDERS</code> in batches of 2,000. At the end, one manual
flush; then poll <code>rocksdb.num-running-compactions</code>, <code>compaction-pending</code> and the flush counters until RocksDB has been idle for 1 s
(RocksJava has no <code>WaitForCompact</code>).</li>
<li><strong>Catching events</strong> (<code>SstEventListener</code>): an <code>AbstractEventListener</code> that turns on only the four callbacks
<code>onFlushBegin</code>, <code>onFlushCompleted</code>, <code>onCompactionBegin</code> and <code>onCompactionCompleted</code>. Each callback calls
<code>db.getLiveFilesMetaData()</code> and prints every live SSTable (name, level, size, smallest key, largest key). <em>Begin</em> gives the
"before" list and <em>Completed</em> gives the "after" list. Compaction inputs are marked <code>*</code> and new files <code>+</code>.
<code>CompactionJobInfo</code> also gives the reason, the input and output levels, and the bytes read and written. A job whose output
files are the same as its inputs is a <em>trivial move</em>: the files are moved to a new level in the MANIFEST without being rewritten.</li>
<li><strong>Index dump</strong> (<code>SstIndex</code>): RocksJava cannot read an index block, so the program decodes the block-based table
format itself. It reads the 53-byte footer (magic <code>0x88e241b785f4cff7</code>) and the index handle, then decodes each index entry
(shared and non-shared key length, the key bytes, and a BlockHandle varint pair or a size delta). It checks the result against the
<code>TableProperties</code> from <code>SstFileReader</code>: exactly one entry per data block, blocks contiguous from offset 0, and
separator keys in ascending order. Every file passed.</li>
<li>Run with <code>bash lab9/run_all.sh</code>. It uses <code>mvn exec:exec</code> to run a separate JVM, because under <code>exec:java</code>
RocksDB's native callback threads cannot find the RocksJava classes (<code>NoClassDefFoundError: org/rocksdb/FlushJobInfo</code>).</li>
</ul>

<h2>4. Results</h2>
{summary}
<h3>4.1 Leveled compactions</h3>
{table(comp_headers, comp["leveled"], numeric={0, 4, 5, 6, 7, 8})}
<h3>4.2 Universal compactions</h3>
{table(comp_headers, comp["universal"], numeric={0, 4, 5, 6, 7, 8})}

<h2>5. Observations</h2>
<ul>
<li><strong>Flushes are the same for both styles.</strong> Each style made {lv['flushes']} flushes ({full} because the write buffer was full, plus {manual} final manual
flush). Each full memtable became one L0 SSTable of about 39.6 MB holding 78,000 orders (about 134 MB of JSON, LZ4-compressed to about 30%).</li>
<li><strong>Leveled: every compaction was a trivial move</strong> ({lv['compactions']} compactions, {lv['compaction_written_bytes']} bytes rewritten,
write amplification {lv['write_amp']}). <code>corders.json</code> is in key order, so each new L0 file covers keys above everything
already stored. It overlaps nothing in the target level, so RocksDB moves the file to a new level without rewriting it (same file
name before and after). With dynamic level bytes, L0 first moves straight to L6. Once L6 is larger than 256 MiB the base level becomes L5,
and when L5 goes over its target a <code>kLevelMaxLevelSize</code> compaction moves L5 files down to L6. The final {lv['final_files']} files
are still exactly the {lv['flushes']} flush outputs.</li>
<li><strong>Universal: real merges, even for sorted input</strong> ({un['compactions']} merges, {int(un['compaction_written_bytes']):,} bytes rewritten,
write amplification {un['write_amp']}). The first compaction is <code>kUniversalSizeAmplification</code>: 4 sorted runs make the size amplification 300%,
above the 200% limit. The later ones are <code>kUniversalSizeRatio</code>. Each compaction rewrites the previous merged run together with the L0 files
flushed since, so the inputs keep growing ({uni_inputs} files). Output to L6 is cut into 64 MiB files (target_file_size_base).
Universal compaction only does trivial moves when <code>allow_trivial_move</code> is turned on (it is off by default), so even
sorted runs that do not overlap were rewritten.</li>
<li><strong>Cost of the merges.</strong> With one compaction thread, the long universal merges let L0 grow to {peak['universal']} files (leveled peaked at {peak['leveled']}), and the
final settle took {float(un['settle_s']):.1f} s against {float(lv['settle_s']):.1f} s for leveled. Universal ends with fewer, larger files
({un['final_levels']}: two sorted runs, below the trigger of 4). Leveled ends with {lv['final_levels']}.</li>
<li><strong>Index.</strong> Each 64 KiB data block (about 19 KB after LZ4) has one index entry: <em>separator key → (offset, size)</em>.
A separator is at least every key in its block and smaller than the first key of the next block, and RocksDB shortens it
where it can (for example {separators} instead of a full 10-digit key). The last entry in each file is the file's largest key.
The index stores user keys (no sequence numbers) and delta-encoded block handles, so a 78,000-order file has a 47 KB index of about 2,100 entries.</li>
</ul>

<h2>6. Program outputs</h2>
{''.join(outputs)}

<h2>7. Source code</h2>
<p>All of <code>lab9/</code>: line-numbered and syntax-highlighted Java, XML and bash.</p>
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
