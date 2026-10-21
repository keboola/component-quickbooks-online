# Experimental report flattener for `keboola.ex-quickbooks-online`

**Date:** 2026-08-26
**Status:** Approved design (pending spec review)

## Problem

For the reports in `report_cant_parse` — `CashFlow`, `ProfitAndLossDetail`,
`TransactionList`, `GeneralLedger`, `TrialBalance` — the component writes the entire
JSON response into a single `value` cell (`ReportMapping.output_1cell`). Users must
parse that JSON downstream, which defeats the purpose for finance use-cases. The other
reports (`BalanceSheet`, `ProfitAndLoss`) are already flattened by the hierarchical
`ReportMapping.parse()`.

## Goal

Add an **opt-in, experimental** flattener that turns those five reports into tabular
rows. Default behaviour must be **byte-for-byte unchanged** — `parse_reports` unset or
`false` produces exactly today's single-JSON-cell output, with the same table names,
columns, primary keys, and state.

## Approved decisions (from brainstorming)

1. **Testing:** unit tests over representative QuickBooks report JSON fixtures (built
   from the public Reports API response schema) for the flattener, plus a regression
   test that the default-`false` path is unchanged. No VCR cassettes — no live QB OAuth
   creds are available, and a pure parser is correctly tested with JSON fixtures, not a
   fabricated HTTP recording.
2. **Schema stability:** within-run column superset via `ElasticDictWriter` only. No
   cross-run persistence in state — that would mean read-modify-write on the state file
   that also holds the OAuth tokens (BC risk), and it matches how the existing parsed
   reports already behave.
3. **Table naming:** reuse today's filenames. When opted in, the *content* of
   `<Report>.csv` / `<Report>_<accounting_type>.csv` changes from one JSON cell to many
   rows/columns. No new/parallel tables.

## Architecture

### New module: `src/report_flattener.py`

A pure, data-driven transformation with no I/O. Flat import style (`from report_flattener
import ...`) to match the rest of `src/`.

Public entry point:

```python
def flatten_report(data: dict, base_row: dict) -> tuple[list[dict], list[str]]:
    """
    Returns (rows, columns).
    rows:    list of output dicts (base cols + ancestry + mapped ColData + row_type
             + row_number).
    columns: ordered superset of column names discovered (base first, then group_*,
             then report columns, then row_type, then row_number). Returned for a
             stable header order; ElasticDictWriter still guarantees a valid union.
    Raises ReportNotFlattenable when the response has no usable Columns/Rows.
    """
```

`base_row` carries the base columns `{ReportName, StartPeriod, EndPeriod}` already built
by `ReportMapping.construct_header`.

#### Column schema

- Read `data["Columns"]["Column"]` (a list). For each column derive an output name:
  1. `ColKey` from `MetaData` — **`MetaData` is a list of `{"Name", "Value"}`**; pick the
     entry whose `Name == "ColKey"`.
  2. Fall back to a slugified `ColTitle`.
  3. Fall back to positional `col_<i>`.
- De-duplicate names (append `_<i>` on collision) so positional mapping stays 1:1.
- Keep the column order from the response.

#### Row walk

Recursively walk `data["Rows"]["Row"]`, carrying **section ancestry** in a list.

For each row:
- **Section** (`type == "Section"`, or has nested `Rows`): push its label onto the
  ancestry and recurse. Label = `Header.ColData[0].value` if present, else the `group`
  attribute, else `""`. After recursing, if the section has a `Summary`, emit it as one
  row (`row_type = "summary"`) with the ancestry *including* this section.
- **Data / ColData-bearing** (`type == "Data"` or `"ColData" in row`): emit one row
  (`row_type = "data"`): base cols + ancestry (as `group_0..group_n`) + each
  `ColData[i].value` mapped to output column `i`.
- **Bare `group` rows** (no `type`, has `group` + `ColData`): treated as a data row whose
  ancestry gets the `group` value — mirrors an existing shape the old parser handles.

Ancestry is emitted as depth-indexed columns `group_0`, `group_1`, … (`group_0` =
outermost). Variable depth across reports is fine — `ElasticDictWriter` unions them.

`row_type` column distinguishes `data` from `summary`. Summary rows are included because
for the summary-shaped reports (`TrialBalance`, `CashFlow`) the totals are the point.

#### Defensive fallback

`flatten_report` raises `ReportNotFlattenable` when `Columns.Column` or `Rows.Row` are
missing/empty. The caller catches it, logs a clear warning naming the report, and writes
the original single-cell JSON blob for that report instead of producing garbage.

### Integration: `src/report_mapping.py`

- `ReportMapping.__init__(..., parse_reports: bool = False)`.
- In the `else` branch (the `report_cant_parse` reports), when `parse_reports` is true:
  call `flatten_report`, write via a new `output_rows()` using
  `keboola.csvwriter.ElasticDictWriter`, with the header row written and a
  `{incremental, primary_key}` manifest (same manifest style as `output()`).
  On `ReportNotFlattenable`, fall back to the existing `output_1cell` path.
- When `parse_reports` is false: unchanged — existing `output_1cell` path, untouched.
- Filenames are produced by the **existing** logic (`<endpoint>.csv` /
  `<endpoint>_<accounting_type>.csv`), so accounting-type splits and names are preserved.

**Primary key** for flattened output: `[ReportName, StartPeriod, EndPeriod, row_number]`,
where `row_number` is a monotonic per-report index emitted by `flatten_report` as it
walks the rows. Per-row PK so an incremental upsert does not collapse all rows into one.
Each accounting-type variant is a separate file, so no cross-file collision.

### Integration: `src/component.py`

- Read `parse_reports = bool(params.get("parse_reports", False))` once in `run()`.
- Pass `parse_reports=parse_reports` to the report `ReportMapping(...)` calls (the
  accrual/cash pair and the single-report call). **Not** to the `CustomQuery` call —
  CustomQuery keeps its dedicated 1-cell behaviour.
- No change to the `**`-suffix handling: `component.py` already strips `**` before
  `ReportMapping` sees the endpoint, so the flattener always receives bare names, and
  `report_cant_parse` (bare names) matches consistently.

### UI: `component_config/configSchema.json`

Add one boolean, default `false`:

```jsonc
"parse_reports": {
  "type": "boolean",
  "title": "Parse reports into rows (experimental)",
  "format": "checkbox",
  "default": false,
  "propertyOrder": <after reports>,
  "description": "Experimental. Flattens reports that are otherwise returned as a single JSON cell (GeneralLedger, TransactionList, ProfitAndLossDetail, CashFlow, TrialBalance) into tabular rows. The column layout follows your QuickBooks report configuration and may change if you change a report's columns or accounting method — not guaranteed stable long-term. Leave off to keep the raw single-cell JSON output (default).",
  "options": { "dependencies": { ... } }
}
```

Dependency strategy (tested with ui-developer + schema-tester / Playwright):
1. **Attempt** an "array contains a hard-to-parse report" dependency so the toggle shows
   only when a `report_cant_parse` report is selected.
2. **Fall back** to `options.dependencies: { "reports": true }` (same pattern as
   `date_settings`) if json-editor won't render the array-contains form — it resolves
   dependencies by scalar equality on an array multiselect. The toggle is a harmless
   no-op for already-parsing reports, and the description scopes it.

Reconcile the schema against the **live Dev Portal** (source of truth for
`configurationSchema`) before finalising; any change is made in-repo (CI-synced from
`component_config/`), never hand-patched in the portal.

## Backwards compatibility (hard requirement)

- `parse_reports` unset/false ⇒ identical output to today via `output_1cell`.
- No change to existing table names, columns, PKs, or state for existing configs.
- CustomQuery, the already-parsed reports (`BalanceSheet`, `ProfitAndLoss`), and all data
  endpoints are untouched.
- The new config property is optional with a `false` default, so existing stored configs
  deserialize unchanged.

## Tests (`unittest`, run via `python -m unittest discover` in Docker)

1. **Regression:** with `parse_reports=False`, a `report_cant_parse` report still yields
   exactly one JSON cell (columns `[…, "value"]`, PK `[ReportName, StartPeriod,
   EndPeriod]`). Assert the produced CSV + manifest are unchanged.
2. **Flatten GeneralLedger:** feed a representative GL response fixture (Header + Columns
   with ColKey `tx_date`/`txn_type`/`doc_num`/`subt_nat_amount` + account Sections with
   nested Data rows) and assert a multi-row, multi-column table with account ancestry
   carried down, correct `row_type`, and the expected PK/columns.
3. **Flatten TransactionList:** a second fixture, one row per transaction.
4. **Summary-shaped validation:** a `TrialBalance` (and/or `CashFlow`) fixture flattens
   to per-account rows + summary rows; confirm it is representable. If a fixture proves a
   report genuinely can't be represented, assert the defensive fallback to the 1-cell
   blob with a warning.
5. **Fallback:** an empty/degenerate response (no Columns/Rows) falls back to the 1-cell
   blob and logs a warning rather than raising.

Fixtures live under `tests/fixtures/` as JSON, built from the public QuickBooks Reports
API response schema.

## Out of scope (note as follow-up in the PR)

Date-chunking large report requests into sub-ranges to avoid timeouts over long periods.
Independent of this change; left out.

## Risks / notes

- Opting in changes a report table's shape. With `incremental_load` on a pre-existing
  1-cell table, a full load (or a fresh table) is advised — documented in the field
  description and PR.
- `MetaData` is a list, not a dict — the flattener must not assume `MetaData.ColKey`.
- `ElasticDictWriter` mutates the `fieldnames` list it is given — pass a copy.
