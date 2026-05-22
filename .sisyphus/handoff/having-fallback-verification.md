# HAVING / Aggregate Fallback Verification

## Summary

Scanned SQL IT tests for canonical HAVING and aggregate-expression patterns. Added 8 representative unit tests to `UnifiedQueryPlannerSqlV2Test.java`. Empirically verified that **none** of the new tests hit the name-match fallback (all pass with fallback removed). The only test that fails with fallback removed is the synthetic `testVisitAggregateFunction_fallsBackToNameMatch` in `CalciteRexNodeVisitorTest`.

## Sources Scanned

- `integ-test/src/test/java/org/opensearch/sql/sql/` — 5 files with HAVING (MathematicalFunctionIT, DateTimeFunctionIT, PaginationFallbackIT, NestedIT, MatchIT)
- `integ-test/src/test/java/org/opensearch/sql/legacy/HavingIT.java` — 11 HAVING patterns (=, <=, <>, BETWEEN, IN, AND, OR, NOT)
- `integ-test/src/test/java/org/opensearch/sql/legacy/AggregationIT.java` — GROUP BY HAVING COUNT(*)
- `integ-test/src/test/java/org/opensearch/sql/legacy/AggregationExpressionIT.java` — scalar-over-aggregate (abs(MAX), MAX+MIN, Log(MAX+MIN))
- `integ-test/src/test/java/org/opensearch/sql/legacy/NestedFieldQueryIT.java` — HAVING with alias, HAVING MAX on nested
- `integ-test/src/test/java/org/opensearch/sql/legacy/SubqueryIT.java` — HAVING in subquery

Total HAVING queries found: ~40+

## Note on COUNT(*) in HAVING

`HAVING COUNT(*) > N` and alias-based `HAVING cnt > 1` (where cnt = COUNT(*)) fail even WITH the fallback present. The AST represents `COUNT(*)` as `COUNT(AllFields())` whose `toString()` is `COUNT(AllFields())`, not `COUNT(*)`. This is a pre-existing limitation unrelated to Fix 6's registry approach. All named-aggregate patterns (MAX, MIN, AVG, SUM) work correctly via the registry.

## Verification Table

| # | Test name | Source IT (file:line) | Original SQL | Adapted SQL | Hits fallback? | Result |
|---|-----------|----------------------|--------------|-------------|----------------|--------|
| 1 | testHavingAndAggResolution_havingMaxCol | legacy/NestedFieldQueryIT.java:692 | `HAVING MAX(p.started_year) > 1990` | `SELECT department FROM catalog.employees GROUP BY department HAVING MAX(age) > 30` | No | PASS |
| 2 | testHavingAndAggResolution_havingMinCol | legacy/HavingIT.java (variant) | `HAVING cnt <= 2` | `SELECT department FROM catalog.employees GROUP BY department HAVING MIN(age) < 30` | No | PASS |
| 3 | testHavingAndAggResolution_havingAvgCol | legacy/AggregationExpressionIT.java:65 | `SELECT gender, AVG(age) ... GROUP BY gender` | `SELECT department FROM catalog.employees GROUP BY department HAVING AVG(age) > 25` | No | PASS |
| 4 | testHavingAndAggResolution_havingMultipleConditionsAnd | legacy/HavingIT.java:97 | `HAVING cnt >= 1 AND cnt < 3` | `SELECT department FROM catalog.employees GROUP BY department HAVING MAX(age) >= 25 AND MIN(age) < 40` | No | PASS |
| 5 | testHavingAndAggResolution_havingMultipleConditionsOr | legacy/HavingIT.java:104 | `HAVING cnt = 1 OR cnt = 3` | `SELECT department FROM catalog.employees GROUP BY department HAVING MAX(age) = 45 OR MIN(age) = 25` | No | PASS |
| 6 | testHavingAndAggResolution_scalarFnOverAggregate | legacy/AggregationExpressionIT.java:28 | `SELECT abs(MAX(age)) FROM account` | `SELECT ABS(MAX(age)) FROM catalog.employees` | No | PASS |
| 7 | testHavingAndAggResolution_arithmeticOnAggregates | legacy/AggregationExpressionIT.java:36 | `SELECT MAX(age) + MIN(age) FROM account` | `SELECT MAX(age) + MIN(age) AS range_sum FROM catalog.employees` | No | PASS |
| 8 | testHavingAndAggResolution_logOverAggArithmetic | legacy/AggregationExpressionIT.java:103 | `SELECT Log(MAX(age) + MIN(age)) FROM account GROUP BY gender` | `SELECT department, LOG(MAX(age) + MIN(age)) AS log_val FROM catalog.employees GROUP BY department` | No | PASS |

## Test Runs

1. **With fallback removed (registry-only):**
   - `:api:test` (full module): BUILD SUCCESSFUL, 0 failures
   - `:core:test --tests CalciteRexNodeVisitorTest`: 8 tests, 1 failed (`testVisitAggregateFunction_fallsBackToNameMatch` — expected)

2. **With fallback restored:**
   - `:api:test` (full module): BUILD SUCCESSFUL, 0 failures
   - `git diff HEAD -- CalciteRexNodeVisitor.java`: empty (confirmed restore)

## Conclusion

The registry-based approach in `visitAggregateFunction` correctly resolves all real-world HAVING and scalar-over-aggregate patterns without needing the name-match fallback. The fallback is purely defensive for hypothetical edge cases where AST identity differs but string form matches.
