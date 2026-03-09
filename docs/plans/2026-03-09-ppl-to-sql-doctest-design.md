# PPL-to-SQL Translator PoC — Doctest via ANSI SQL Engine

**Date:** 2026-03-09
**Branch:** `poc/ppl-to-sql-doctest` (cut from `poc/extend-calcite-sql-select`)
**Goal:** Pass 85% of PPL doctest queries by translating PPL→SQL and executing via the ANSI SQL engine.

## Problem

PPL doctests currently execute via `/_plugins/_ppl` → PPLService → CalciteRelNodeVisitor → RelNode.
We want to validate that the ANSI SQL engine behind `/_plugins/_sql` can serve PPL queries by:
1. Translating PPL to ANSI SQL
2. Executing the SQL through the existing unified query API
3. Returning results in the same format

## Architecture

```
                    ┌─────────────────────────────────┐
                    │  POST /_plugins/_pplv4           │
                    │  {"query": "source=t | where…"}  │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │  RestPPLV4QueryAction            │
                    │  1. Parse PPL query              │
                    │  2. PPLToSqlTranspiler.transpile()│
                    │  3. Forward SQL to unified API   │
                    │     (RestUnifiedSQLQueryAction)  │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │  Existing ANSI SQL Engine        │
                    │  SqlParser → SqlNode → RelNode   │
                    │  → PreparedStatement → Execute   │
                    └─────────────────────────────────┘
```

## Components

### 1. PPLToSqlTranspiler (Java, `api/` module)
- Cherry-pick from `poc/ppl-to-sqlnode-unified-api` branch
- Extends `AbstractNodeVisitor<String, Void>`
- Parses PPL via `PPLSyntaxParser` → AST → walks nodes → produces SQL string
- Contains inner `SelectBuilder` class for SQL assembly
- Handles ~20 commands, 100+ function mappings
- Will be extended incrementally to cover more commands

### 2. RestPPLV4QueryAction (Java, `legacy/` module)
- New REST handler registered at `/_plugins/_pplv4`
- Receives PPL query in same format as `/_plugins/_ppl`
- Calls `PPLToSqlTranspiler.transpile(pplQuery)` to get SQL
- Delegates SQL execution to `RestUnifiedSQLQueryAction` logic
- Returns result in same JDBC JSON format
- No fallback to PPL engine — fail explicitly if translation fails

### 3. Doctest Changes (Python, `doctest/` module)
- New category `ppl_cli_v4` in `docs/category.json` (or modify existing `ppl_cli_calcite`)
- New transform function `ppl_v4_markdown_transform()` that wraps queries as `pplv4_cmd.process('query')`
- New `DocTestConnection` instance `pplv4_cmd` with endpoint `/_plugins/_pplv4`
- Minimal changes to `opensearch_connection.py` to support the new endpoint

## Iteration Strategy

Build translator incrementally by command priority, run full doctest suite after each batch.

### P0 — Foundation (18% of examples, ~65 queries)
Commands: `search`, `where`, `fields`, `head`, `sort`, `stats`, `eval`, `table`
SQL features: SELECT, FROM, WHERE, ORDER BY, LIMIT, GROUP BY, expressions

### P1 — Standard SQL (33% more, ~120 queries)
Commands: `top`, `rare`, `regex`, `rename`, `dedup`, `join`, `subquery`, `append`
SQL features: ROW_NUMBER, UNION ALL, JOIN variants, subqueries

### P2 — Window/CTE (15% more, ~55 queries)
Commands: `eventstats`, `trendline`, `reverse`, `lookup`, `appendpipe`, `graphlookup`
SQL features: Window functions, CTE, recursive CTE, LEFT JOIN + COALESCE

### P3 — EXCEPT/REPLACE (14% more, ~50 queries)
Commands: `fillnull`, `replace`, `rex`, `parse`, `flatten`, `expand`, `mvexpand`
SQL features: SELECT * EXCEPT, SELECT * REPLACE, UNNEST

### P4 — Complex (14% more, ~50 queries) — stretch goal
Commands: `bin`, `streamstats`, `chart`, `timechart`, `mvcombine`, `grok`, `spath`

### P5 — TVF-only (6%, ~25 queries) — out of scope
Commands: `patterns`, `transpose`, `addcoltotals`, `fieldformat`, `nomv`
These cannot translate to SQL — mark as `ignore` in doctest.

**Target: P0-P3 = ~80% + partial P4 = 85%+**

## Files Changed

| File | Action | Module |
|------|--------|--------|
| `api/src/.../PPLToSqlTranspiler.java` | Cherry-pick + extend | api |
| `legacy/src/.../RestPPLV4QueryAction.java` | New | legacy |
| `plugin/src/.../SQLPlugin.java` | Register new endpoint | plugin |
| `doctest/test_docs.py` | Add pplv4 connection + category | doctest |
| `doctest/markdown_parser.py` | Add ppl_v4 transform | doctest |
| `doctest/sql-cli/.../opensearch_connection.py` | Support pplv4 endpoint | doctest |
| `docs/category.json` | Add ppl_cli_v4 category | docs |
| `progress.txt` | Removed (loop artifact) | root |
| `docs/dev/select-except-replace-loop.md` | Removed (loop artifact) | docs |

## Success Criteria

- `/_plugins/_pplv4` endpoint accepts PPL, returns SQL results
- 85%+ of PPL doctest queries pass when routed through `/_plugins/_pplv4`
- No changes to the existing `/_plugins/_sql` or `/_plugins/_ppl` endpoints
- Translation failures return clear error messages (not silent fallback)

## Non-Goals

- No server-side PPL engine changes
- No fallback to PPL engine
- No TVF support (P5 commands)
- No production readiness — this is a PoC
