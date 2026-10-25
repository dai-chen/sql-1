# PoC Design: Staged Calcite Execution on OpenSearch Data Nodes

Status: PoC design, approved for implementation
Date: 2026-08-17
Related: [opensearch-project/sql#5698](https://github.com/opensearch-project/sql/issues/5698) option 4

## Problem

The V3 (Calcite) PPL engine translates as much of the Calcite plan as it can into OpenSearch Query DSL. Two failure modes follow from that strategy:

1. **Expressiveness.** DSL cannot express large parts of relational algebra. When translation falls short the engine either refuses the query or emits a Painless script query, which is a second, semantically divergent implementation of the same expression language.
2. **Pagination.** When an operator cannot absorb the limit, the scan below it must return every matching document. That exceeds `max_result_window`, so a PIT is opened. Over a wildcard index pattern this opens one reader context per matching shard across hundreds of daily indices and exhausts `search.max_open_pit_context` (default 300).

`dedup` is the canonical instance of (2), and it is worked through in detail below.

This design inverts the strategy: the plan is **executed** on data nodes as a Calcite plan, and DSL translation is reduced to a narrow optimization.

## Tenets

1. **Execution is the default; translation is an optimization.** DSL translation may fail freely — the consequence is slower, never unsupported. This converts *capability* failures ("unsupported operation", a dead end for the user) into *resource* failures ("this query needed 40M rows on one node"), which are diagnosable and actionable.
2. **The split is total by construction.** Absence of an optimization rule means an operator runs on the coordinator, correctly. It is never a coverage gap.
3. **Push only what the index accelerates.** If a predicate does not consult an inverted index, BKD tree, or norms, pushing it buys nothing. No Painless script queries, ever.
4. **One semantic limit in the plan; physical caps only where provably exact; a budget that refuses rather than truncates.** No silent approximation anywhere.
5. **One search request per index scan. Never a cursor.** Multiple bounded requests are categorically different from pagination.
6. **Standard Calcite operators only.** Calcite's Enumerable rule set cannot execute custom logical operators, so they must not exist.

## Architecture

```
PPL text → AstBuilder → CalciteRelNodeVisitor          [standard operators only]
                             ↓
                    LogicalSystemLimit(10000)          [existing; it IS the semantic limit]
                             ↓
                    AggregateReduceFunctionsRule       [AVG→SUM/COUNT; MUST precede split]
                             ↓
                    OpenSearchIndexScan + PredicateAnalyzer
                             ↓
                    StagePlanner                       [generic floor + 4 optional rules]
                             ↓
        ┌────────────────────┼─────────────────────────┐
   shardFragment      combineDescriptor          coordinatorTree
   (RelJson base64)   (declarative, 5 modes)     (in-memory RelNode)
        ↓                    ↓                          ↓
   calcite_exec agg    InternalCalciteExec        plugin, post-SearchResponse
   per shard            .reduce()   [tier 1]      [tier 2]
```

### Component inventory

Everything below is required for the PoC. Nothing is inherited from prior work.

| Component | Module | Responsibility |
|---|---|---|
| `PredicateAnalyzer` | `opensearch` | the **only** Rel→DSL translation; index-accelerable predicates only |
| `OpenSearchIndexScan` | `opensearch` | single physical scan carrying `(index, queryDsl, projects)` |
| `StagePlanner` | `core` | computes placement, applies split rules, cuts the DAG |
| `CalciteExecAggregationBuilder` | `opensearch` | `AggregationBuilder`; fields `plan`, `fields`, `combine`, `rowBudget`; XContent + `Writeable` |
| aggregation registration | `opensearch` | `SearchPlugin.getAggregations()` → `AggregationSpec(NAME, reader, parser).addResultReader(InternalCalciteExec::new)` |
| `CalciteExecAggregatorFactory` / `CalciteExecAggregator` | `opensearch` | single-bucket aggregator; `collect(doc)` buffers a row, `buildAggregation()` runs the fragment |
| shard row source | `opensearch` | per-type doc_values readers, `_source` fallback, budget counter |
| fragment codec | `opensearch` | RelJson ⇄ base64; deserialization needs a `RelOptCluster`, a schema exposing the shard row-source table, and the plugin operator table |
| shard executor | `opensearch` | Janino-compiled `Bindable` under `CalciteClassLoaderHelper.withCalciteClassLoader()`; rows injected through a `DataContext` stash slot read by a `ScannableTable` |
| `InternalCalciteExec` | `opensearch` | `InternalAggregation`; carries rows/accumulator state, the echoed combine descriptor, and collection stats; `reduce()` applies the descriptor |
| tier-2 executor | `opensearch` | runs `coordinatorTree` through Calcite over gathered rows |

### Minimal-change notes

- `plugins.calcite.pushdown.enabled` and `plugins.calcite.fallback.allowed` **already exist** (`common/src/main/java/org/opensearch/sql/common/setting/Settings.java:42-47`). Tenet 1 and the coverage measurement are configuration flips, not new code.
- `LogicalSystemLimit` already wraps every plan (`core/src/main/java/org/opensearch/sql/executor/QueryService.java:1032-1036`) and already extends `Sort`, so it is optimizer-visible and already matched by `LimitIndexScanRule` (`opensearch/src/main/java/org/opensearch/sql/opensearch/planner/rules/LimitIndexScanRule.java:24`). Keep it. Do not add a second limit concept.
- Before writing `PredicateAnalyzer`, inventory the existing DSL predicate translation and reuse it. Do not author a new translator; restrict the existing one and delete its script-query path.
- Explain goldens are YAML under `integ-test/src/test/resources/expectedOutput/calcite/`, loaded via `loadExpectedPlan(...)` (`PPLIntegTestCase:484`). Re-baselining is editing YAML.

## Key decisions

### One physical scan operator

`OpenSearchIndexScan(index, queryDsl, projects)`. "Table scan" and "index scan" are the same operator; `match_all` is the degenerate case. No code path exists solely for the unaccelerated case, which is where divergence bugs breed.

### The pushable set is closed and small

Push a predicate only if it consults an index structure:

| Pushed | Structure |
|---|---|
| `match`, `match_phrase`, `query_string`, relevance functions | inverted index, scoring — and *cannot* be evaluated from doc_values at all |
| term / terms equality | inverted index, global ordinals |
| range on numeric or date | BKD tree, plus `can_match` shard skipping |
| `exists` / `IS NOT NULL` | field-names, norms |
| conjunctions of the above | — |

Everything else — arithmetic, string functions, `case`, regex, disjunctions that do not decompose — stays as a residual `Filter` inside the shard fragment and runs as compiled Java. A residual filter does **not** block staging; it is shard-local.

The range row is what closes #5698: a `@timestamp` range pushed as a real range query enables `can_match`, so a wildcard pattern prunes non-matching daily indices before any shard work.

Predicates go in `bool.filter` (filter context: cacheable, no scoring). The exception is a query ranking by relevance (`_score`), where they must move to `must` so scores are computed.

### StagePlanner is a rewrite pass, not Volcano trait plumbing

Trino's `AddExchanges` is itself a plan rewrite visitor rather than an iterative rule set, so there is direct precedent, and it is substantially less machinery for identical results. The trait-based formulation is where this goes *if* cost-based push decisions ever become necessary — they are not, because pushing an index-accelerable predicate is monotonically better.

One property, computed bottom-up:

```
SHARD_LOCAL   – subtree runs on a shard; concatenating per-shard
                results is a valid input to the parent
NEEDS_GATHER  – this node requires all rows in one place
```

Generic floor, zero per-operator work:

| Node | Placement |
|---|---|
| `OpenSearchIndexScan` | `SHARD_LOCAL` |
| `Project` / `Calc` / `Filter` | `SHARD_LOCAL` if input is (distribution-preserving) |
| **everything else** | `NEEDS_GATHER` — the default, and why the split is total |

The cut is placed at the highest `SHARD_LOCAL` node. Everything above becomes `coordinatorTree` with the scan replaced by a gathered-rows scan.

### Four optional decomposition rules

Each fires only when a `NEEDS_GATHER` node sits directly on a `SHARD_LOCAL` input. Each is an optimization that cannot affect coverage.

| # | Rule | Precedent |
|---|---|---|
| 1 | `Aggregate` → partial + final, iff every agg call is splittable | Calcite `SqlSplittableAggFunction`; Spark `AggUtils`; Trino `AggregationNode.Step` |
| 2 | `Sort`+`Fetch` → shard top-N + coordinator merge | Spark `TakeOrderedAndProjectExec`; Trino `TopNNode.Step` |
| 3 | `Filter(rank ≤ k)` over `Window(ROW_NUMBER/RANK/DENSE_RANK)` → partial rank limit | Spark `InferWindowGroupLimit` / `WindowGroupLimitExec` (SPARK-37099); Trino `TopNRankingNode(partial=true)` |
| 4 | `Fetch(N)`, unordered → shard `Fetch(N)` + early termination | Trino `MergeLimitWithSort` family |

Rule 3's correctness is rank monotonicity: adding rows to a partition can only push a row's rank higher, so a locally-disqualified row is globally disqualified.

Only `Aggregate` and top-N receive bespoke partial/final treatment in any surveyed engine (Calcite, Ignite, Flink, Spark, Trino, CockroachDB). Per-operator decomposition rules are the industry norm, not a design smell. `eventstats` gets no rule because unbounded-frame aggregate windows admit no partial anywhere; same for `LAG`/`LEAD`, moving frames, `NTILE`, `PERCENT_RANK`, `CUME_DIST`.

### Two-tier reduce

`InternalAggregation.reduce()` is called in batches of `batched_reduce_size` (default 512) by `QueryPhaseResultConsumer.PendingReduces`. Anything inside it **must be associative** and must carry **unfinalized** accumulator state. There is no opt-out. A general RelNode tree — a window, a global sort, a join — is not associative; putting one there yields silently wrong answers past 512 shards, which is exactly the wildcard-daily-index case.

| Tier | Where | Contract | Content |
|---|---|---|---|
| 1 | `InternalCalciteExec.reduce()` | associative | one of five declarative descriptors |
| 2 | plugin, after `SearchResponse` | none | `coordinatorTree`, executed once |

Descriptors: `CONCAT`, `MERGE_AGG{groupKeys, aggs}`, `TOP_N{keys, dirs, n}`, `RANK_LIMIT{partKeys, orderKeys, k}`, `LIMIT{n}`. Tier 1 needs no serialized RelNode, which is why no reduce fragment appears in the DSL. The descriptor travels in the request and the shard echoes it back inside `InternalCalciteExec`, because the reducing node may not be the plugin node that built the plan.

Batched reduce genuinely lowers peak memory — `partialReduce()` calls `consumeAggs().expand()`, `onAfterReduce()` rebases the breaker to `reduceResult.estimatedSize - estimatedSize`, and the buffer is `clear()`'d — **but only when the combine reduces volume.** `CONCAT` gains nothing, which is precisely the case the budget guards.

### Shard bridge: buffer rows plus a budget counter

A shard-global counter in `collect()` with two throw sites:

- **Exact early termination**, when rule 4 established a legitimately pushable limit: `CollectionTerminatedException` plus a guard in `getLeafCollector()`. `CollectionTerminatedException` alone is **per-segment only** — it is caught by `ContextIndexSearcher#searchLeaf()` and the leaf loop continues — so stopping a whole shard requires both. Core precedent is `EarlyTerminatingCollector` with `forceTermination=true`.
- **Safety budget breach**: a hard exception naming the operator that forced the gather.

`allowPartialSearchResults(false)` is **mandatory** on every request. The default is `true`, so a budget breach would otherwise return HTTP 200 with `_shards.failures` and partial results — the exact silent-wrong-answer this design exists to eliminate. Also call `addRequestCircuitBreakerBytes` for buffered state so the cluster sees the query's footprint.

Two plan shapes need different shard structures, which is why a `Fetch` node alone cannot express this:

| Shape | Shard structure | Docs examined | Shard memory |
|---|---|---|---|
| `head N`, unordered | counter → early terminate | stops at N | N rows |
| `sort X \| head N` | bounded priority queue of size N | all matching | N rows |

**Known limitation, accepted for the PoC.** The bridge remains buffer-then-execute, so a `Fetch` in the fragment bounds what is *emitted*, not what is *collected*. The budget bounds the failure; it does not make the common case fast. The correct end-state bridge collects compact doc IDs and materializes rows lazily as Calcite pulls — mirroring OpenSearch's own query-phase/fetch-phase division — which is what makes rule 4 reduce *work* rather than only wire bytes. Out of PoC scope. Its constraint, for the record: doc_values iterators are forward-only per segment, so lazy materialization must walk segments in order and doc IDs ascending within each.

### One search request per index scan

Single-index queries issue one request; a join issues one per scan and joins on the coordinator. Neither needs a cursor. Multiple bounded requests are not pagination — cursor state exists to stream an *unbounded* result across round trips.

## Custom logical operator audit (US-001)

Does tenet 6 ("standard Calcite operators only") already hold on the Calcite path?

| PPL command(s) | Custom RelNode class | File path | Lowers to standard operators before optimization? |
|---|---|---|---|
| all queries, `join`, `lookup`, `union`, subqueries | `LogicalSystemLimit` | `core/src/main/java/org/opensearch/sql/calcite/plan/rel/LogicalSystemLimit.java:25` | No — persists as a `Sort` subclass (fetch-only, no ordering). Matched by `LimitIndexScanRule`. |
| `dedup` (mid-optimization only) | `LogicalDedup` | `core/src/main/java/org/opensearch/sql/calcite/plan/rel/LogicalDedup.java:20` | Yes — `PPLDedupConvertRule` lowers back to `LogicalFilter` + `LogicalProject` (ROW_NUMBER window) + `LogicalSort`. |
| `graphLookup` | `LogicalGraphLookup` | `core/src/main/java/org/opensearch/sql/calcite/plan/rel/LogicalGraphLookup.java:22` | **No** — irreducible; no standard equivalent exists. |
| *(dead code, never instantiated)* | `TimeWindow` | `core/src/main/java/org/opensearch/sql/calcite/plan/TimeWindow.java:15` | N/A — dead code. |

**Verdict.** `LogicalSystemLimit` is a `Sort` subclass present in every plan and already matched by `LimitIndexScanRule`; it satisfies the spirit of tenet 6.  `LogicalDedup` is transient during HEP optimization and is lowered back to standard operators by `PPLDedupConvertRule`; it satisfies tenet 6.  `LogicalGraphLookup` (from the `graphLookup` command) is irreducible and therefore the one genuine tenet-6 violation — it persists through optimization and is converted directly to a physical `CalciteEnumerableGraphLookup`.

- All other PPL commands — `where`, `fields`, `eval`, `stats`, `eventstats`, `streamstats`, `sort`, `head`, `dedup`, `rare`/`top`, `trendline`, `appendcol`, `expand`, `flatten`, `mvexpand`, `patterns`, `parse`, `grok`, `rex`, `spath`, `fillnull`, `rename`, `bin`, `lookup`, `join`, `union`, `append`, `reverse`, `transpose`, `addtotals`, `chart`/`timechart`, `mvcombine`, `nomv`, `replace`, `regex`, `makeresults`, `values`, and the rest of the grammar — produce only standard Calcite logical operators.  The audit walked every command rule in `ppl/src/main/antlr/OpenSearchPPLParser.g4` (lines 58–105) so is complete over the command grammar, not merely over the discovered classes.

### dedup lowers to a standard window plus rank filter

The translation entry point is `CalciteRelNodeVisitor.visitDedupe()` (`core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java:2208`).  The lowering rule `PPLDedupConvertRule` (`core/src/main/java/org/opensearch/sql/calcite/plan/rule/PPLDedupConvertRule.java`) dispatches to `buildDedupNotNull()` (line 151) or `buildDedupOrNull()` (line 114).  Both use `ROW_NUMBER` exclusively — never `RANK` or `DENSE_RANK`.

Explain goldens under `integ-test/src/test/resources/expectedOutput/calcite/` confirm the lowered form:

```
LogicalFilter(condition=[<=($3, 1)])
  LogicalProject(..., _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $1)])
    LogicalFilter(condition=[IS NOT NULL($1)])
```

IT methods that load these goldens: `ExplainIT.testDedupPushdown()` (line 489), `ExplainIT.testDedupKeepEmptyTrueNotPushed()` (line 499), `ExplainIT.testDedupKeepEmptyFalsePushdown()` (line 509), `CalciteExplainIT.testComplexDedup()` (line 2214), `CalciteExplainIT.testDedupExpr()` (line 2238).

`consecutive=true` throws `CalciteUnsupportedException` at `CalciteRelNodeVisitor.java:2217`; no golden exists for it.

## Worked example 1: `dedup` — the #5698 failure, and its fix

```
source=accounts | dedup gender | fields account_number, gender, age
```

### Today

Logical plan (verbatim, from the existing no-pushdown golden):

```
LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
  LogicalProject(account_number=[$0], gender=[$1], age=[$2])
    LogicalFilter(condition=[<=($3, 1)])
      LogicalProject(account_number=[$0], gender=[$1], age=[$2], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $1)])
        LogicalFilter(condition=[IS NOT NULL($1)])
          LogicalProject(account_number=[$0], gender=[$4], age=[$8])
            CalciteLogicalIndexScan(table=[[OpenSearch, opensearch-sql_test_index_account]])
```

Physical plan:

```
EnumerableLimit(fetch=[10000])
  EnumerableCalc(...)
    EnumerableWindow(window#0=[window(partition {4} rows between UNBOUNDED PRECEDING and CURRENT ROW aggs [ROW_NUMBER()])])
      EnumerableCalc(...)
        CalciteEnumerableIndexScan(table=[[OpenSearch, opensearch-sql_test_index_account]])
```

`EnumerableWindow` runs **in the plugin, on the coordinator**. The limit sits above the window and cannot be pushed below it, so `CalciteEnumerableIndexScan` must return every matching document. Beyond `max_result_window` that triggers PIT (`OpenSearchRequestBuilder.java:127-140`), and over a wildcard pattern the PIT-per-shard fan-out exhausts `search.max_open_pit_context`. This single query shape is #5698.

### Staged

`IS NOT NULL(gender)` is a candidate for the `exists` query (see caveat below). `Filter(rank ≤ 1)` over `ROW_NUMBER() OVER (PARTITION BY gender)` is exactly rule 3's pattern.

```
shardFragment:
  LogicalProject(account_number=[$0], gender=[$1], age=[$2])          ← drops the rank column
    LogicalFilter(condition=[<=($3, 1)])
      LogicalProject(account_number=[$0], gender=[$1], age=[$2],
                     _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $1)])
        LogicalProject(account_number=[$0], gender=[$4], age=[$8])
          OpenSearchIndexScan(table=[[OpenSearch, opensearch-sql_test_index_account]],
                              query=[{"exists":{"field":"gender"}}])

combine:  RANK_LIMIT{partitionKeys:[1], k:1}       ← recomputes rank over the union

coordinatorTree:
  LogicalSystemLimit(fetch=[10000], type=[QUERY_SIZE_LIMIT])
    LogicalProject(account_number=[$0], gender=[$1], age=[$2])
      «gathered rows»
```

Two details that matter:

**The shard must drop `_row_number_dedup_` before shipping.** Shard-local rank values are meaningless to the coordinator, which has to re-rank over the union of shard outputs. `RANK_LIMIT` therefore *recomputes* rank rather than filtering the shipped column — for `k=1` that is a hash-distinct on the partition keys, for `k>1` it is "keep first k per key". Both are associative, so tier 1 is legal. Shipping the column would be pure waste.

**Why the partial is correct here is nondeterminism, not monotonicity.** `ROW_NUMBER() OVER (PARTITION BY gender)` has no `ORDER BY`, so which row receives rank 1 is unconstrained; any one row per partition is a valid answer, and composing per-shard choices with a coordinator choice yields another valid answer. Rule 3's general justification — rank monotonicity, where adding rows can only push a rank higher — is what covers the `RANK`/`DENSE_RANK`-with-`ORDER BY` cases. Both arguments must hold for the rule to fire; the implementation should assert which one applies.

**Caveat on the `exists` push.** `IS NOT NULL` and OpenSearch `exists` are not unconditionally equivalent — `null_value` mappings, empty strings, and multi-valued fields can diverge. If equivalence is not certain for a field's mapping, leave the predicate as a residual filter in the fragment. Under tenet 1 that costs only speed, which is precisely the freedom this design buys; the previous engine had to get it right or refuse the query.

Request:

```json
{
  "size": 0,
  "query": { "bool": { "filter": [ {"exists": {"field": "gender"}} ] } },
  "aggs": { "calcite_stage": { "calcite_exec": {
      "plan": "<base64 RelJson shardFragment>",
      "fields": [{"name":"account_number","type":"long"},
                 {"name":"gender","type":"keyword"},
                 {"name":"age","type":"long"}],
      "combine": {"mode":"RANK_LIMIT","partitionKeys":[1],"k":1},
      "row_budget": 200000
  }}}
}
```
with `allowPartialSearchResults(false)`.

Each shard returns **at most one row per distinct `gender`** — two or three rows — instead of every matching document. No PIT, no cursor, one request. The wire volume goes from O(documents) to O(distinct keys × shards).

## Worked example 2: `stats` — aggregate decomposition

```
source=logs-* | where status = 500 and lower(host) like 'web%'
| stats count() as c by service | sort - c | head 10
```

`PredicateAnalyzer` splits the conjunction: `status = 500` becomes a term query; `lower(host) LIKE 'web%'` is not index-accelerable and stays as a residual filter inside the fragment. Rule 1 fires because `COUNT` is splittable (`CountSplitter`). `Sort`+`Fetch(10)` sits above the *final* aggregate and a limit cannot push below an `Aggregate`, so it stays on the coordinator.

```
shardFragment:    Aggregate(group={service}, pc=COUNT())
                    ← Project(service)
                      ← Filter(LOWER(host) LIKE 'web%')
                        ← OpenSearchIndexScan(logs-*, {"term":{"status":500}})

combine:          MERGE_AGG{groupKeys:[0], aggs:[SUM(1)]}     ← this IS the final aggregate

coordinatorTree:  Sort(c DESC, fetch=10) ← «gathered rows»
```

One row per distinct `service` per shard. Because `AggregateReduceFunctionsRule` ran pre-split, tier 1 needs no finalization: had this been `avg`, the shard would ship `{sum, count}` and the division would appear as a `Project` in `coordinatorTree`.

## Worked example 3: `eventstats` — the ceiling

```
source=logs-* | eventstats avg(latency) as avg_latency by service | where latency > avg_latency
```

The window is `NEEDS_GATHER` with no applicable rule, so the cut falls to the `Project` beneath it, the combine is `CONCAT`, and every matching row gathers. On a large index this trips the budget:

```
eventstats requires all rows on one node; gathered 12,400,000 rows, budget 200,000
```

A resource failure naming its cause, not an OOM and not a silently truncated average.

## Acceptance test plan

Verification runs with `plugins.calcite.pushdown.enabled=false` and `plugins.calcite.fallback.allowed=false`, so every query exercises the staged path and no query can silently escape to the old engine.

Commands:

```bash
./gradlew build -x :integ-test:integTest                 # build
./gradlew test                                           # unit tests
./gradlew :integ-test:integTest                          # all ITs
./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalciteDedupCommandIT"
```

### Phase A — execution correctness

Ten existing IT classes, ~148 test methods, chosen to cover the typical PPL surface and all four rules plus the ceiling:

| PPL shape | IT class (`org.opensearch.sql.calcite.remote.`) | Tests | Exercises |
|---|---|---|---|
| `fields` | `CalciteFieldsCommandIT` | 39 | generic floor |
| `where` | `CalciteWhereCommandIT` | 6 | pushed + residual predicates |
| `eval` | `CalciteEvalCommandIT` | 10 | shard-local expression evaluation |
| `head` | `CalciteHeadCommandIT` | 6 | rule 4, early termination |
| `sort` | `CalcitePPLSortIT` | 18 | rule 2 |
| `top` | `CalciteTopCommandIT` | 5 | rules 1 + 2 composed |
| `stats` | `CalciteStatsCommandIT` | 4 | rule 1, `MERGE_AGG` |
| `dedup` | `CalciteDedupCommandIT` | 5 | rule 3, `RANK_LIMIT` |
| `eventstats` | `CalcitePPLEventstatsIT` | 27 | `CONCAT` path and the ceiling |
| explain | `CalciteExplainIT` | ~28 | Phase B |

Results are reported in **three buckets**, never conflated:

- **pass**
- **fail-on-assertion** — a real correctness gap, or an `explain` golden needing re-baseline
- **fail-on-ceiling** — the gather budget refused the query

The bucket counts are the PoC's primary deliverable. Conflating the last two would obscure the only number that matters.

#### Measured baseline (US-010)

Measured on 2026-08-19 on branch `poc/staged-calcite-exec` with `plugins.calcite.pushdown.enabled=false` and `plugins.calcite.fallback.allowed=false` set as transient cluster settings. Each of the ten IT classes was run through a `StagedCalcite*` subclass that sets that posture in `init()` and restores both settings to `null` in `tearDown()`. Every number below was read from the JUnit XML in `integ-test/build/test-results/integTest/`, not from a Gradle exit code.

| Staged IT class | Tests | Skipped | pass | fail-on-assertion | fail-on-ceiling |
|---|---|---|---|---|---|
| StagedCalciteFieldsCommandIT | 39 | 0 | 39 | 0 | 0 |
| StagedCalciteWhereCommandIT | 41 | 3 | 38 | 0 | 0 |
| StagedCalciteEvalCommandIT | 9 | 0 | 9 | 0 | 0 |
| StagedCalciteHeadCommandIT | 6 | 2 | 4 | 0 | 0 |
| StagedCalcitePPLSortIT | 18 | 0 | 18 | 0 | 0 |
| StagedCalciteTopCommandIT | 9 | 0 | 0 | 9 | 0 |
| StagedCalciteStatsCommandIT | 63 | 0 | 37 | 26 | 0 |
| StagedCalciteDedupCommandIT | 5 | 0 | 1 | 4 | 0 |
| StagedCalcitePPLEventstatsIT | 27 | 0 | 1 | 26 | 0 |
| StagedCalciteExplainIT | 266 | 80 | 186 | 0 | 0 |
| **Total** | **483** | **85** | **333** | **65** | **0** |

**Annotation of the 65 fail-on-assertion entries:**

| Cause | Count | Classes | Annotation |
|---|---|---|---|
| `CannotPlanException` — no ENUMERABLE conversion rules for the plan | 33 | Eventstats 26, Top 6, Dedup 1 | suspected real defect; the window/aggregate plan has no enumerable implementation on the staged path |
| `UnsupportedOperationException` | 12 | Stats 9, Dedup 3 | suspected real defect |
| RelJson `cannot serialize enum value to JSON: SqlTypeName.<X>` (BIGINT ×7, DECIMAL ×1, INTEGER ×1) | 9 | Stats 9 | suspected real defect in fragment serialization of aggregate call types |
| Shard `NullPointerException` (`SearchPhaseExecutionException`) | 8 | Stats 5, Top 3 | suspected real defect |
| `Unable to implement EnumerableCalc` (`IllegalStateException`) | 2 | Stats 2 | suspected real defect |
| `can not write type [class java.math.BigDecimal]` (`IllegalArgumentException`) | 1 | Stats 1 | suspected real defect in the staged response writer |

**Caveats — read before quoting the 333/483 number:**

- **fail-on-ceiling is zero, and that is not yet evidence the ceiling works.** No query in these ten classes reached the 200 000-row budget, so the row-budget refusal path (US-009) is exercised only by its own dedicated IT, not by this baseline.
- **The explain result is narrower than it looks.** 76 of `StagedCalciteExplainIT`'s 80 skips are the suite's own `assumeTrue` guard, "This test is only for when push down is enabled". So explain coverage under the PoC posture is 186 of 266 tests, and the remaining 4 skips are pre-existing `@Ignore`s. Zero explain goldens needed re-baselining because the stage split is not rendered in explain yet — that is US-011's job.
- **The other skips are inherited, not new.** `StagedCalciteHeadCommandIT`'s 2 skips are `@Ignore`s on the parent `org.opensearch.sql.ppl.HeadCommandIT` (issue 703); `StagedCalciteWhereCommandIT`'s 3 skips predate this measurement.
- **Stats/Top/Dedup/Eventstats correspond exactly to the unimplemented split rules.** Their staged subclasses carry a class-level `@Ignore` naming the unblocking story (US-012 for Aggregate partial/final and MERGE_AGG, US-014 for Sort+Fetch top-N, US-015 for the window rank-filter) so CI stays green; that story removes the annotation. `Fields`, `Where`, `Eval`, `Head`, `PPLSort` and `Explain` run unignored as active regression gates.
- **The doc's earlier per-class test counts are stale.** The table above records the counts actually observed; e.g. `stats` is 63 tests, not 4, and `top` is 9, not 5.

The generic placement floor executes project/filter/eval/sort/limit shapes correctly end to end (333 passing), and no plan was ever rejected by the ceiling — every failure is a missing enumerable implementation or a serialization gap on the staged path, consistent with Design Invariant 1.

### Phase B — explain shows the split

`explain` output must render the three parts distinctly: `shardFragment`, `combine`, `coordinatorTree`. This is how a reviewer confirms the design works as intended rather than trusting row counts. New/updated YAML goldens under `integ-test/src/test/resources/expectedOutput/calcite/` for at minimum:

- `where` + `fields` (floor only, `CONCAT`)
- `stats ... by` (rule 1, `MERGE_AGG`)
- `dedup` (rule 3, `RANK_LIMIT`)
- `sort ... | head N` (rule 2, `TOP_N`)
- `head N` (rule 4, `LIMIT`)
- `eventstats` (no rule, `CONCAT`, full gather)

### Phase C — the generated DSL is correct

Assert on the captured `SearchRequest` that: `size == 0`; `allowPartialSearchResults == false`; the `query` clause contains exactly the index-accelerable conjuncts; residual predicates are **absent** from the query clause and **present** in the fragment; no PIT, scroll, or `search_after` is ever created on this path.

### Phase D — ceiling behaviour

With `row_budget` set very low: the query fails with an error naming the row count and the forcing operator; the response contains no partial results; the circuit breaker recorded the buffered bytes.

### Phase E — the partial actually reduces

`InternalCalciteExec` carries collection stats (rows collected, rows emitted). Assert that for `dedup` and `stats`, rows emitted per shard is strictly less than documents matched — proving the shard did real work rather than passing rows through, which is the specific defect this design replaces.

## Results (US-016)

Final state of the PoC on branch `poc/staged-calcite-exec` at commit `d621a10c3` plus this story. Measured 2026-08-26 with `plugins.calcite.pushdown.enabled=false` and `plugins.calcite.fallback.allowed=false`, every class in **one** `:integ-test:integTest --rerun-tasks` invocation (Gradle wipes the results directory per invocation, so batched runs are unverifiable), every number read from JUnit XML archived to `.sisyphus/handoff/us016-xml/`. Cluster health confirmed before trusting the run: no `classMethod` pseudo-test in any XML, no mid-run node shutdown.

### Final coverage — the three buckets

| Staged IT class | Tests | Skipped | pass | fail-on-assertion | fail-on-ceiling |
|---|---|---|---|---|---|
| StagedCalciteFieldsCommandIT | 39 | 0 | 39 | 0 | 0 |
| StagedCalciteWhereCommandIT | 41 | 3 | 38 | 0 | 0 |
| StagedCalciteEvalCommandIT | 9 | 0 | 9 | 0 | 0 |
| StagedCalciteHeadCommandIT | 6 | 2 | 4 | 0 | 0 |
| StagedCalcitePPLSortIT | 18 | 0 | 18 | 0 | 0 |
| StagedCalciteTopCommandIT | 9 | 0 | 9 | 0 | 0 |
| StagedCalciteStatsCommandIT | 63 | 0 | 59 | 4 | 0 |
| StagedCalciteDedupCommandIT | 5 | 0 | 5 | 0 | 0 |
| StagedCalcitePPLEventstatsIT | 27 | 0 | 27 | 0 | 0 |
| StagedCalciteExplainIT | 266 | 80 | 186 | 0 | 0 |
| **Total** | **483** | **85** | **394** | **4** | **0** |

Against the US-010 baseline measured on the same ten classes: **333 pass / 65 fail-on-assertion → 394 pass / 4 fail-on-assertion**. Every row satisfies `tests − skipped == pass + fail + ceiling`; the total does too (`483 − 85 = 398 = 394 + 4`).

Nine further classes ran in the same invocation as regression gates, all at zero failures: non-staged `CalciteExplainIT` 266/4 skipped, `CalciteStatsCommandIT` 63, `CalciteDedupCommandIT` 5, `CalciteTopCommandIT` 9, `CalcitePPLEventstatsIT` 27; staged `StagedCalcitePPLJoinIT` 43, `StagedCalciteUnionCommandIT` 15; plus `CalciteStageSplitExplainIT` 5 and `CalciteExecAggregationIT` 9. Whole invocation: **925 tests, 89 skipped, 4 failures**, and the 4 are the ones in the table.

Skips are unchanged in character from US-010 and none are new: 76 of `StagedCalciteExplainIT`'s 80 are the suite's own `assumeTrue("This test is only for when push down is enabled")`, so staged explain coverage is 186 of 266, not 266; the remaining 4 plus Head's 2 and Where's 3 are inherited `@Ignore`s that predate the PoC.

**The 4 remaining fail-on-assertion entries, with verified root causes.** `StagedCalciteStatsCommandIT` carries a class-level `@Ignore` in CI naming these four; the numbers above were taken with the annotation temporarily removed.

| Test | Symptom | Root cause |
|---|---|---|
| `testStatsTimeSpan`, `testStatsSpanSortOnMeasure` | shard `IllegalStateException: Unable to implement EnumerableAggregate` | **Verified by unit reproduction.** The suppressed exception is `IllegalArgumentException: Unsupported expr type: TIMESTAMP`, thrown by `SpanFunction.SpanImplementor.implement`, which requires the field type to be a PPL `ExprSqlType` UDT (`EXPR_TIMESTAMP`). `RelFragmentCodec.osTypeToSqlType` deliberately maps `date` to a plain `TIMESTAMP` because the UDT wrappers do not survive the RelJson round-trip, so the implementor falls through to its throw. |
| `testStatsBySpanTimeWithNullBucket` | HTTP 400 `ExpressionEvaluationException: timestamp:1753661723000 in unsupported format` | Same root family. Staged rows carry temporal values as epoch millis, while `SpanFunction.evalTimestamp(String, int, String)` expects the UDT's formatted string form. |
| `testStatsSortOnMeasureComplex` | HTTP 500 `UnsupportedOperationException`, empty details | The query uses `dc(employer)`. `DISTINCT_COUNT_APPROX` is a **logical marker** whose accumulator throws on every method (`core/.../calcite/udf/udaf/DistinctCountApproxLogicalAggFunction.java:26-58`); the real HyperLogLog++ implementation is injected into `PPLFuncImpTable`'s external registry by `OpenSearchExecutionEngine`, and neither the shard fragment compiler nor `CoordinatorTreeExecutor` applies that override. Whether the marker is reached on the shard or on the coordinator was not isolated. |

So the two causes behind all four are (i) the staged wire drops PPL UDT typing and the temporal value form that PPL UDF implementors dispatch on, and (ii) logical-marker aggregate functions resolved through a registry the staged compilers do not consult. Neither is a limit of the split model.

**`fail-on-ceiling` is zero, and that is still not evidence the ceiling works.** The largest index in these ten classes is `accounts` at 1 000 documents against a 200 000-row budget, so nothing came within two orders of magnitude of the refusal path. That path is exercised only by `CalciteExecAggregationIT.testRowBudgetBreachFailsFast`, which sets `row_budget: 2` on the 7-document `bank` index and asserts the exact message `gathered 3 rows, budget 2` with `allow_partial_search_results=false` on the URL.

### What actually gets staged

Census over the 209 goldens in `expectedOutput/calcite_no_pushdown/` (171 YAML, 38 JSON; the `json_tree` format deliberately does not render the sections):

| combine | Goldens | Rule |
|---|---|---|
| `LIMIT` | 67 | rule 4 — dominant because the `LogicalSystemLimit(QUERY_SIZE_LIMIT)` wrapping every plan is itself an unordered fetch and is promoted |
| `MERGE_AGG` | 40 | rule 1 |
| `CONCAT` | 39 | generic placement floor, full gather |
| `TOP_N` | 4 | rule 2 |
| `RANK_LIMIT` | 2 | rule 3 |
| none (coordinator-only) | 19 YAML | see below |

152 of 171 YAML goldens render a split. The 19 that do not: 12 are multi-scan shapes (`join`, `union`, `append`, `multisearch`) which have no single gather boundary; 5 are the `search_with_*` relative-time shapes whose plans contain a relevance function that has no row-level enumerable implementor; 2 are `chart_single_group`/`chart_multiple_groups`, an `avg`-with-sort shape whose refusal cause was **not isolated** in this story. Every one of these still executes, entirely on the coordinator — Design Invariant 1 holds by refusing to stage, never by rejecting.

### Ceiling map

The row budget counts **buffered** rows (`CalciteExecAggregator.collect` throws when `rowsCollected > rowBudget`), not shipped rows. Tier-1 combines reduce wire volume; they do **not** reduce shard memory. The only mechanism that bounds collection is rule 4's `earlyTerminationLimit`, set only when every node between the promoted unordered fetch and the scan is cardinality-preserving (`StagePlannerTest.earlyTerminationLimit_is_non_null_for_fetch_over_project_over_scan` and `..._is_null_when_filter_sits_between_fetch_and_scan` pin both directions).

| PPL shape | combine | Bounds shard buffering | Breach volume at the 200 000 default | Observed |
|---|---|---|---|---|
| `fields`, `eval` (no residual filter) | `LIMIT{10000}` | early termination at the system limit | never — collection stops at 10 000 rows/shard | 0 breaches, 48 tests |
| `head N` unordered over a plain scan | `LIMIT{n}` | early termination at n | never | 0 breaches, 6 tests (4 run) |
| `where` with a residual filter | `LIMIT` | nothing — the filter is inside the fragment, so termination is unsafe | > 200 000 matching docs/shard | 0 breaches, 41 tests |
| `sort … \| head N` | `TOP_N` | nothing — the n-th key is unknown until all matches are examined | > 200 000 matching docs/shard | 0 breaches, 18 + 9 tests |
| `stats … by` | `MERGE_AGG` | nothing — the partial aggregate still consumes every row | > 200 000 matching docs/shard | 0 breaches, 63 tests |
| `dedup` | `RANK_LIMIT` | nothing — a window must see the whole partition | > 200 000 matching docs/shard | 0 breaches, 5 tests |
| `eventstats`, non-rank window | `CONCAT` | nothing | > 200 000 matching docs/shard | 0 breaches, 27 tests |
| nested-field queries | any | nothing; `rowsCollected` counts post-expansion rows | > 200 000 / mean sub-documents per parent | 0 breaches |
| `join`, `union`, `append`, `multisearch` | not staged | no `calcite_exec` is issued at all | n/a | 0 breaches, 58 tests |

No PPL shape exceeded the budget at IT data volumes (7 to 1 000 documents, single-shard indices). The breach threshold is the same for every shape except the two early-terminating ones, which is the honest form of this map: staging changes **what crosses the wire**, and only rule 4 changes **what a shard holds**.

### Wire-volume deltas (Phase E)

Asserted on the shard's own `rowsCollected`/`rowsEmitted` counters inside the `calcite_exec` response, before any coordinator reduce could mask the reduction. Both use the 7-document `bank` index with the low-cardinality `gender` key (2 distinct values).

| Shape | combine | Documents matched | Rows emitted per shard | Test |
|---|---|---|---|---|
| `dedup gender` | `RANK_LIMIT{partitionKeys:[1], k:1}` | 7 | 2 | `CalciteExecAggregationIT.testDedupFragmentEmitsFewerRowsThanItCollects` |
| `stats count() by gender` | `MERGE_AGG{groupKeys:[0], aggs:[SUM]}` | 7 | 2 | `CalciteExecAggregationIT.testStatsFragmentEmitsOneRowPerGroupKey` |

Both are O(distinct keys) rather than O(documents), and the `stats` case additionally asserts that the unfinalized per-group counts sum to 7 — the partial carries accumulator state, not a finalized answer. This is the specific behaviour that replaces issue #5698's ship-everything-then-paginate: the row volume crossing the wire is now proportional to key cardinality, and no PIT is created anywhere on this path.

### Remaining gaps

Ordered by how far each is from being fixable inside this design. The first four need neither a shuffle nor the option-C bridge.

| Gap | Effect today | What it needs |
|---|---|---|
| The wire drops PPL UDT typing and the temporal value form | Any fragment containing a UDF whose implementor dispatches on `ExprSqlType` fails to compile (`SPAN`: 3 tests) | Local fix: carry the `ExprType` on `FieldDescriptor` and rebuild UDT-typed row types on the shard, or give the affected implementors a plain-SQL-type branch. Both sides of the symmetric-`Writeable` checklist apply. |
| Logical-marker aggregates | `dc`/`distinct_count` throws (1 test) | Local fix: apply `PPLFuncImpTable`'s external implementor registry in the shard fragment compiler and in `CoordinatorTreeExecutor`. |
| Staging and DSL pushdown never coexist | The staged path is gated on `pushdown.enabled == false`, so a staged fragment never carries a pushed query clause and the `shardFragment` explain section always renders an empty DSL. Relevance functions therefore force a coordinator-only plan (5 goldens) rather than being pushed. | Local but non-trivial: let `StagePlanner` run with pushdown rules registered, so the scan inside the fragment carries its DSL and relevance predicates ride in the query clause instead of the fragment. |
| `OpenSearchRestClient` path | `IllegalStateException("Staged execution requires a NodeClient")` | Local fix: a REST-side response parser that preserves the raw `InternalCalciteExec`. |
| Multi-scan plans: `join`, `union`, `append`, `multisearch` | Coordinator-only (12 goldens, 58 tests still correct) | More than one gather boundary. A small-side broadcast needs multiple fragments per query; a large×large join needs a **shuffle**. The stage model generalizes to N stages, so this is additive rather than blocked. |
| Non-rank `Window` (`eventstats`), global sort without a limit, high-cardinality `stats` | `CONCAT`/`MERGE_AGG` gather; correct, but bounded by one coordinator's heap and refused by the budget beyond it | A **shard-to-shard shuffle**, to repartition by the window's `PARTITION BY` key or the sort key. OpenSearch has no such primitive; this is the ~15–25% class named under Known limitations. |
| Shard memory is O(matching docs) for every shape except an early-terminating unordered limit | The budget converts an OOM into a diagnosable refusal; it does not make the query work | The **option-C lazy doc-ID bridge** — stream rows into the fragment instead of buffering them. It is the only listed option that removes the buffer rather than bounding its failure. |
| Fields under two different `nested` paths | Cross-product, documented in code and not silent | Local fix: correlated expansion, or a refusal. |
| Rule-3 refusals: `rank = k` for k > 1, unordered `RANK`/`DENSE_RANK`, `dedup keepempty=true`; rule-4 refusal: non-zero offset | Fall through to `CONCAT`, so results stay correct and only the optimization is lost | Each is a correctness boundary, not an omission — see the rule 3 and rule 4 sections. `keepempty=true` would need a nulls-pass-through flag on the descriptor. |
| `chart_single_group` / `chart_multiple_groups` are coordinator-only | 2 goldens; results correct | Cause not isolated. Recorded as open rather than guessed. |

### Verdict

The PoC's central claim was totality: that a split which defaults to `NEEDS_GATHER` never turns a missing optimization into a query failure. That held. Across 398 executed staged tests no plan was ever rejected by the planner and no query was silently truncated; 4 failures remain and both of their causes are missing function implementations on the staged compile paths, not gaps in the split. The `#5698` shape specifically is fixed: `dedup` now ships 2 rows per shard where it previously shipped every matching document through a PIT, and no PIT, scroll, or `search_after` is created anywhere on this path.

## Out of scope

Lazy doc-ID bridge (option C); approximate/truncating mode for high-cardinality `stats`; shard-to-shard shuffle; cost-based push decisions; routing-key-aligned window optimization; performance parity with today's pushdown.

## Known limitations

- **Performance will regress broadly** when the posture is first flipped, recovering as rules 1–4 land. The valuable output is the coverage number and the ceiling map, not a benchmark.
- **The budget bounds failure, not memory.** Option B still materializes up to the budget. This PoC makes large queries fail cleanly and diagnosably; it does not make them work.
- **OpenSearch has exactly one non-trivial distribution.** With no shard-to-shard shuffle, the only synthesizable distribution is a gather to the coordinator. Window (non-rank), global sort without a limit, and large×large joins are therefore permanently bounded by one node's heap, where Spark and Trino scale them out by shuffling. Roughly 15–25% of observability queries fall in that class (inference, not measured). Closing it requires a real streaming layer in core; the stage model generalizes to N stages, so it does not preclude that.
- **`explain` goldens change wholesale.** Expect Phase A's fail-on-assertion bucket to be dominated by re-baselining rather than by real defects, especially before Phase B lands.

## Evidence appendix

Facts established during design, with sources, that the implementation must not contradict.

| Fact | Source |
|---|---|
| `size: 0` sets `hasTopDocs=false`, skipping the fetch phase, so no reader context or PIT is needed | OpenSearch query phase |
| Reduce is batched at `batched_reduce_size` (512); `reduce()` must be associative; no opt-out exists | `QueryPhaseResultConsumer.PendingReduces` |
| `mustReduceOnSingleInternalAgg()` only forces `reduce()` when a single shard returned; it does not disable batching | `InternalAggregation` |
| Batched reduce lowers peak memory only when the combine reduces volume | `partialReduce()`, `onAfterReduce()` |
| `Aggregator.collect()` can read `_source`, independent of the fetch phase, so no-doc_values fields are reachable | `SearchContext.lookup().getLeafSearchLookup(ctx).source()` |
| Transport ceiling data node → coordinator is 30% of JVM heap, hardcoded; there is no `transport.max_message_size` setting | `TcpTransport` |
| `http.max_content_length` (100MB) binds only the final response; the plugin reads `SearchResponse` in process | — |
| `CollectionTerminatedException` terminates the current leaf only | `ContextIndexSearcher#searchLeaf()` |
| `allow_partial_search_results` defaults to `true` | `search.default_allow_partial_results` |
| Calcite's aggregate split bottoms out per function; `unwrap(SqlSplittableAggFunction.class) == null` means unsplittable | `SqlSplittableAggFunction`, `CountSplitter`, `SumSplitter`, `Sum0Splitter`, `SelfSplitter` |
| Window rank-filter has a standard partial; general and unbounded-frame windows do not | Spark `InferWindowGroupLimit` (SPARK-37099); Trino `AddExchanges.visitTopNRanking()` |
| Limit-pushability is per-operator: exact below `Project`/`Calc`, merges with `Sort`, exact per-branch for `Union ALL`; not below `Filter`, `Aggregate`, general `Window`, `Join`, `Union DISTINCT` | Calcite `SortProjectTransposeRule`, `SortUnionTransposeRule` |
| `terms` agg approximation is bespoke `TermsAggregator` code, not a lifecycle feature; per-shard truncation is semantically valid only when `Sort`+`Fetch` sits above the `Aggregate` | `doc_count_error_upper_bound`, `sum_other_doc_count` |
| PIT today triggers on `startFrom + size > maxResultWindow`, or any paginated query | `OpenSearchRequestBuilder.java:127-140` |
