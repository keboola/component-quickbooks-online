"""
Regression tests for the per-page streaming output path (SUPPORT-16682).

The OOM fix replaced the "accumulate every page in memory, then write once" pattern
with a generator that yields one page at a time, flushed straight to disk through a
shared ``TableStreamWriter``. These tests guard the two properties that refactor must
not break:

1. Pagination invariance - splitting the same records across several pages produces
   BYTE-IDENTICAL output to processing them all at once. This is the core risk: that
   chunking + append-mode writing could drop rows, duplicate headers, or misalign
   columns (e.g. when a nested sub-table first appears only on a later page).

2. Parity with the legacy path - the streamed output is logically identical (same
   tables, same column order, same rows) to the original accumulate-then-write
   implementation that shipped to customers.

The fixture is built around the ``Purchase`` endpoint (the one that OOMed in the
ticket) precisely because it has nested sub-tables that appear sparsely: page 1 has
no line items at all, so ``Purchase-Line`` and friends are first created mid-stream
on page 2.
"""

import csv
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

# The component uses flat imports (``from mapping import ...``); put ``src`` on the path.
SRC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
sys.path.insert(0, SRC_DIR)

import mapping  # noqa: E402
from mapping import Mapping, TableStreamWriter  # noqa: E402

ENDPOINT = "Purchase"


def _purchase(pid, **extra):
    """Builds a minimal-but-realistic Purchase API record; ``extra`` overrides/adds fields."""
    record = {
        "Id": pid,
        "AccountRef": {"value": "33", "name": "Checking"},
        "PaymentType": "Cash",
        "TotalAmt": 100,
        "MetaData": {"CreateTime": "2024-01-01T00:00:00-08:00", "LastUpdatedTime": "2024-01-02T00:00:00-08:00"},
        "TxnDate": "2024-01-01",
        "CurrencyRef": {"value": "USD"},
    }
    record.update(extra)
    return record


def _line(line_id, description, amount):
    return {
        "Id": line_id,
        "Description": description,
        "Amount": amount,
        "DetailType": "AccountBasedExpenseLineDetail",
        "AccountBasedExpenseLineDetail": {
            "AccountRef": {"value": "7", "name": "Expenses"},
            "BillableStatus": "NotBillable",
        },
    }


def build_pages():
    """
    Three pages where the nested sub-tables only appear from page 2 onwards - this is
    the column-misalignment trap the streaming writer must survive.
    """
    page1 = [
        # No line items, no tax detail, missing optional PrivateNote (sparse row).
        _purchase("1001", TotalAmt=100),
        # PrivateNote contains a comma + quote to exercise CSV quoting.
        _purchase("1002", TotalAmt=250.5, PrivateNote='note, with "comma"'),
    ]
    page2 = [
        _purchase("1003", TotalAmt=75, Line=[_line("1", "Item A", 50), _line("2", "Item B", 25)]),
        _purchase(
            "1004",
            TotalAmt=312.5,
            Line=[_line("1", "Only line", 300)],
            TxnTaxDetail={
                "TotalTax": 12.5,
                "TaxLine": [
                    {
                        "Amount": 12.5,
                        "DetailType": "TaxLineDetail",
                        "TaxLineDetail": {"PercentBased": True, "TaxPercent": 5, "NetAmountTaxable": 250},
                    }
                ],
            },
        ),
    ]
    page3 = [
        _purchase("1005", TotalAmt=42, Line=[_line("1", "Late page item", 42)]),
    ]
    return [page1, page2, page3]


class _DeterministicUUID:
    """Replacement for uuid.uuid4 that yields a stable, repeatable sequence.

    Sub-table primary keys are built from ``uuid4().hex``. To compare two runs we need
    the same records (in the same order) to produce the same keys, so each run gets a
    fresh instance whose counter starts at zero.
    """

    def __init__(self):
        self.n = 0

    def __call__(self):
        value = types.SimpleNamespace(hex=f"uuid{self.n:06d}")
        self.n += 1
        return value


def _run_streaming(pages, out_dir):
    writer = TableStreamWriter(destination=out_dir + os.sep)
    with mock.patch("mapping.uuid.uuid4", new=_DeterministicUUID()):
        try:
            for page in pages:
                Mapping(endpoint=ENDPOINT, data=page, writer=writer)
        finally:
            writer.close()


def _run_legacy(rows, out_dir):
    # The legacy path reads the module-level destination constant directly.
    with mock.patch.object(mapping, "DEFAULT_FILE_DESTINATION", out_dir + os.sep), mock.patch(
        "mapping.uuid.uuid4", new=_DeterministicUUID()
    ):
        Mapping(endpoint=ENDPOINT, data=rows)  # no writer -> legacy pandas single-shot


def _read_table(path):
    with open(path, newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    header = rows[0] if rows else []
    data = rows[1:] if len(rows) > 1 else []
    return header, data


def _read_dir(out_dir):
    return {name for name in os.listdir(out_dir) if name.endswith(".csv")}


def _norm_cell(value):
    """Normalises a CSV cell so int/float formatting differences (pandas '75.0' vs
    csv '75') don't cause spurious mismatches when comparing against the legacy path."""
    try:
        return ("num", float(value))
    except (TypeError, ValueError):
        return ("str", value)


def _norm_rows(rows):
    return [[_norm_cell(c) for c in row] for row in rows]


class StreamingMappingTest(unittest.TestCase):
    def setUp(self):
        self.pages = build_pages()
        self.all_rows = [r for page in self.pages for r in page]

    def test_pagination_is_byte_identical_to_single_shot(self):
        """Streaming records across 3 pages == streaming them all in one page, byte-for-byte."""
        with tempfile.TemporaryDirectory() as paged_dir, tempfile.TemporaryDirectory() as single_dir:
            _run_streaming(self.pages, paged_dir)
            _run_streaming([self.all_rows], single_dir)

            self.assertEqual(_read_dir(paged_dir), _read_dir(single_dir), "different set of output tables")
            self.assertTrue(_read_dir(paged_dir), "no output tables were produced")

            for name in _read_dir(paged_dir):
                with open(os.path.join(paged_dir, name), "rb") as a, open(os.path.join(single_dir, name), "rb") as b:
                    self.assertEqual(a.read(), b.read(), f"{name} differs between paged and single-shot streaming")

    def test_late_appearing_subtable_has_one_header(self):
        """A sub-table first produced on page 2 must still have exactly one header row."""
        with tempfile.TemporaryDirectory() as out_dir:
            _run_streaming(self.pages, out_dir)
            line_path = os.path.join(out_dir, "Purchase-Line.csv")
            self.assertTrue(os.path.exists(line_path), "Purchase-Line.csv was not created")
            header, data = _read_table(line_path)
            # Header is the mapping's destinations + the parent-table FK.
            self.assertIn("parent_table", header)
            self.assertEqual(len(header), len(set(header)), "duplicate columns in header")
            # 3 line items total (2 on page 2 record 1003, 1 on 1004, 1 on page 3) -> 4 rows.
            self.assertEqual(len(data), 4)
            # No stray header rows leaked into the data (append-mode regression guard).
            self.assertNotIn(header, data)

    def test_streaming_matches_legacy_output(self):
        """Streamed output is logically identical to the original accumulate-then-write path."""
        with tempfile.TemporaryDirectory() as stream_dir, tempfile.TemporaryDirectory() as legacy_dir:
            _run_streaming([self.all_rows], stream_dir)
            _run_legacy(self.all_rows, legacy_dir)

            self.assertEqual(_read_dir(stream_dir), _read_dir(legacy_dir), "different set of output tables")

            for name in _read_dir(stream_dir):
                stream_header, stream_rows = _read_table(os.path.join(stream_dir, name))
                legacy_header, legacy_rows = _read_table(os.path.join(legacy_dir, name))
                self.assertEqual(stream_header, legacy_header, f"{name}: column set/order differs from legacy")
                self.assertEqual(
                    _norm_rows(stream_rows),
                    _norm_rows(legacy_rows),
                    f"{name}: row content differs from legacy",
                )

    def test_root_table_row_count(self):
        """Sanity: every Purchase record reaches the root output table exactly once."""
        with tempfile.TemporaryDirectory() as out_dir:
            _run_streaming(self.pages, out_dir)
            _, data = _read_table(os.path.join(out_dir, "Purchase.csv"))
            self.assertEqual(len(data), len(self.all_rows))


if __name__ == "__main__":
    unittest.main()
