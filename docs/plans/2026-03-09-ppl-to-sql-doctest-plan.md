# PPL-to-SQL Doctest PoC — Implementation Plan

**Design:** [docs/plans/2026-03-09-ppl-to-sql-doctest-design.md](./2026-03-09-ppl-to-sql-doctest-design.md)

## Phase 1: Scaffold (endpoint + transpiler + doctest wiring)

- [ ] 1.1 Cherry-pick `PPLToSqlTranspiler.java` from `poc/ppl-to-sqlnode-unified-api` into `api/src/main/java/org/opensearch/sql/api/`
- [ ] 1.2 Create `RestPPLV4QueryAction.java` in `legacy/` — REST handler at `/_plugins/_pplv4` that calls transpiler and forwards SQL to unified API
- [ ] 1.3 Register `/_plugins/_pplv4` endpoint in `SQLPlugin.java`
- [ ] 1.4 Build and verify the Java side compiles: `./gradlew :api:compileJava :legacy:compileJava :plugin:compileJava`
- [ ] 1.5 Wire doctest: add `pplv4` endpoint support in `opensearch_connection.py`, add `ppl_v4_markdown_transform` in `markdown_parser.py`, add `pplv4_cmd` in `test_docs.py`, add `ppl_cli_v4` category in `category.json`
- [ ] 1.6 Run doctest on a single simple doc (e.g., `where.md`) to verify end-to-end wiring works

## Phase 2: P0 commands — Foundation (~65 queries)

- [ ] 2.1 Verify/fix transpiler for: `search`, `where`, `fields`, `head`, `sort`, `stats`, `eval`, `table`
- [ ] 2.2 Run full doctest suite, record baseline pass rate
- [ ] 2.3 Fix translation issues found in P0 commands

## Phase 3: P1 commands — Standard SQL (~120 queries)

- [ ] 3.1 Extend transpiler for: `top`, `rare`, `regex`, `rename`, `dedup`, `join`, `subquery`, `append`
- [ ] 3.2 Run full doctest suite, record pass rate
- [ ] 3.3 Fix translation issues found in P1 commands

## Phase 4: P2 commands — Window/CTE (~55 queries)

- [ ] 4.1 Extend transpiler for: `eventstats`, `trendline`, `reverse`, `lookup`, `appendpipe`, `graphlookup`
- [ ] 4.2 Run full doctest suite, record pass rate
- [ ] 4.3 Fix translation issues found in P2 commands

## Phase 5: P3 commands — EXCEPT/REPLACE (~50 queries)

- [ ] 5.1 Extend transpiler for: `fillnull`, `replace`, `rex`, `parse`, `flatten`, `expand`, `mvexpand`
- [ ] 5.2 Run full doctest suite, record pass rate
- [ ] 5.3 Fix translation issues found in P3 commands

## Phase 6: P4 commands — Complex (stretch, ~50 queries)

- [ ] 6.1 Extend transpiler for highest-value P4 commands: `bin`, `streamstats`, `chart`, `timechart`
- [ ] 6.2 Run full doctest suite — target 85%+

## Phase 7: Finalize

- [ ] 7.1 Mark untranslatable queries (P5/TVF) as `ignore` in markdown docs
- [ ] 7.2 Final doctest run with pass rate report
- [ ] 7.3 Update design doc with final results
- [ ] 7.4 Commit all changes

## Build & Test Commands

```bash
# Java build
./gradlew :api:compileJava :legacy:compileJava :plugin:compileJava

# Full build with tests
./gradlew build -x test -x integTest

# Doctest (all PPL v4)
./gradlew doctest -Pcategory=ppl_cli_v4

# Doctest (specific file)
./gradlew doctest -Pdocs=where

# Doctest (specific files)
./gradlew doctest -Pdocs=where,head,sort,stats
```
