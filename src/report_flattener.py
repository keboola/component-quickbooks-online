"""
Pure, I/O-free flattener for QuickBooks Report API responses that the existing
hierarchical ``ReportMapping.parse`` cannot handle (``report_cant_parse`` in
``report_mapping.py``: ``CashFlow``, ``ProfitAndLossDetail``, ``TransactionList``,
``GeneralLedger``, ``TrialBalance``).

Opt-in via ``parse_reports``. See
docs/superpowers/specs/2026-08-26-report-flattener-design.md for the full design.
"""

import itertools
import re


class ReportNotFlattenable(Exception):
    """Raised when a report response has no usable Columns/Rows to flatten."""


def flatten_report(data: dict, base_row: dict) -> tuple[list[dict], list[str]]:
    """
    Flattens a QuickBooks Report API response into tabular rows.

    Args:
        data: the raw report response (``Header``/``Columns``/``Rows``).
        base_row: base columns already built by ``ReportMapping.construct_header``
            (``ReportName``, ``StartPeriod``, ``EndPeriod``), carried on every row.

    Returns:
        (rows, columns) - ``rows`` is a list of flat dicts (base cols + ``group_*``
        section ancestry + report columns + ``row_type`` + ``row_number``); ``columns``
        is a stable, ordered superset of the column names discovered: base columns,
        then ``group_*``, then report columns, then ``row_type``, then ``row_number``.
        It is a starting header for the caller - an ``ElasticDictWriter`` still
        guarantees a valid union if individual rows vary.

    Raises:
        ReportNotFlattenable: if ``Columns.Column`` or ``Rows.Row`` is missing/empty.
    """
    columns_meta = (data.get("Columns") or {}).get("Column")
    rows_meta = (data.get("Rows") or {}).get("Row")

    if not columns_meta:
        raise ReportNotFlattenable("Report response has no usable 'Columns.Column'.")
    if not rows_meta:
        raise ReportNotFlattenable("Report response has no usable 'Rows.Row'.")

    column_names = _resolve_column_names(columns_meta)

    rows: list[dict] = []
    row_numbers = itertools.count(1)
    _walk_rows(rows_meta, [], base_row, column_names, row_numbers, rows)

    max_depth = 0
    for row in rows:
        depth = sum(1 for key in row if key.startswith("group_"))
        max_depth = max(max_depth, depth)

    columns = list(base_row.keys())
    columns.extend(f"group_{i}" for i in range(max_depth))
    columns.extend(column_names)
    columns.extend(["row_type", "row_number"])

    return rows, columns


def _walk_rows(rows_in, ancestry, base_row, column_names, row_numbers, rows_out):
    """
    Recursively walks ``Rows.Row``, carrying section ancestry down, appending one flat
    dict per emitted row (data or summary) to ``rows_out``.
    """
    for row in rows_in:
        row_type = row.get("type")
        is_section = row_type == "Section" or "Rows" in row

        if is_section:
            section_ancestry = ancestry + [_section_label(row)]
            nested_rows = (row.get("Rows") or {}).get("Row") or []
            _walk_rows(nested_rows, section_ancestry, base_row, column_names, row_numbers, rows_out)

            summary = row.get("Summary")
            if summary:
                rows_out.append(
                    _build_row(
                        base_row,
                        section_ancestry,
                        summary.get("ColData") or [],
                        column_names,
                        "summary",
                        next(row_numbers),
                    )
                )

        elif row_type == "Data" or "ColData" in row:
            row_ancestry = ancestry
            if row_type is None and "group" in row:
                # Bare `group` row: no `type`, has `group` + `ColData` directly.
                row_ancestry = ancestry + [row["group"]]

            rows_out.append(
                _build_row(
                    base_row,
                    row_ancestry,
                    row.get("ColData") or [],
                    column_names,
                    "data",
                    next(row_numbers),
                )
            )
        # Rows matching none of the shapes above are skipped defensively - one
        # unrecognised row does not make the whole report unflattenable.


def _section_label(row: dict) -> str:
    """Label for a Section row: Header.ColData[0].value, else `group`, else ''."""
    header_col_data = (row.get("Header") or {}).get("ColData") or []
    if header_col_data:
        return header_col_data[0].get("value", "")
    return row.get("group", "")


def _build_row(base_row, ancestry, col_data, column_names, row_type, row_number) -> dict:
    row = dict(base_row)
    for i, label in enumerate(ancestry):
        row[f"group_{i}"] = label
    for i, name in enumerate(column_names):
        row[name] = col_data[i].get("value") if i < len(col_data) else None
    row["row_type"] = row_type
    row["row_number"] = row_number
    return row


def _resolve_column_names(columns_meta: list) -> list:
    """
    Derives one output column name per report column, preserving response order:
    1. `ColKey` from `MetaData` (a list of ``{"Name", "Value"}`` entries, not a dict).
    2. Slugified `ColTitle`.
    3. Positional `col_<i>`.
    Collisions are de-duplicated with a positional suffix (`_<i>`) so the mapping
    stays 1:1 with the response column order.
    """
    names = [
        _col_key(column) or _slugify(column.get("ColTitle", "")) or f"col_{i}"
        for i, column in enumerate(columns_meta)
    ]

    seen = set()
    deduped = []
    for i, name in enumerate(names):
        if name in seen:
            name = f"{name}_{i}"
        seen.add(name)
        deduped.append(name)

    return deduped


def _col_key(column: dict) -> str:
    for entry in column.get("MetaData") or []:
        if entry.get("Name") == "ColKey":
            return entry.get("Value") or ""
    return ""


def _slugify(title: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", title.strip()).strip("_").lower()
