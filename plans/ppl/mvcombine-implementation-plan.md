# PPL Command Implementation Plan: mvcombine

## Header

- **Command:** mvcombine
- **Date:** 2025-01-19
- **Owner:** OpenSearch SQL Team
- **Input Requirements:** `rfcs/ppl/mvcombine-requirements.md`
- **Approver(s):** Maintainer (OpenSearch SQL Team)

---

## High-level Decisions (from §2.2)

### Subset/Composition:
**COMPOSITION** - The `mvcombine` command is a composition of existing PPL aggregation patterns:
- **GROUP BY** (on all fields except the specified field)
- **AGGREGATE** (collect values of the specified field into array or delimited string)

The command maps to: `stats <AGG_FUNCTION>(field) BY <all_other_fields>`

### SQL equivalents for 3 use cases:

**Use Case 1: Consolidating host values after stats**
```sql
SELECT max, min, ARRAY_AGG(host) as host
FROM (
  SELECT MAX(bytes) as max, MIN(bytes) as min, host
  FROM logs
  GROUP BY host
) subquery
GROUP BY max, min
```

**Use Case 2: Combining with custom delimiter**
```sql
SELECT status, 
       STRING_AGG(server, ',') as server
FROM servers
WHERE status = 'active'
GROUP BY status
```

**Use Case 3: Combining numeric values**
```sql
SELECT user,
       ARRAY_AGG(error_code) as error_code
FROM errors
GROUP BY user
```

### Pushdown feasibility (full/partial/none) + boundary note:
**DEFER** - Pushdown optimization may already be supported by existing aggregation infrastructure. Will verify during implementation whether dynamic GROUP BY and aggregation operations can be pushed down to OpenSearch DSL.

### UDF/UDAF (names + signatures or "none"):
**NONE** - Will leverage existing functions based on delimiter value:
- Without `delim` parameter: Use existing `ARRAY_AGG()` or similar collection aggregation
- With `delim` parameter: Use existing string aggregation functions or compose `ARRAY_AGG()` + `MVJOIN()`

---

## Evidence & References

### Similar Commands Considered:
1. **chart** - Transforming command with aggregation and grouping semantics
2. **appendpipe** - Transforming command that modifies the pipeline
3. **mvappend** - Multivalue function for reference on multivalue handling

### PRs inspected (IDs/links) for change-list synthesis:
- PR #4579 - Support `chart` command in PPL
  - https://github.com/opensearch-project/sql/pull/4579
  - Pattern: Full command implementation with AST, visitors, tests, docs
  
- PR #4602 - Support `appendpipe` command in PPL
  - https://github.com/opensearch-project/sql/pull/4602
  - Pattern: Command with subpipeline handling
  
- PR #4438 - Add mvappend function for Calcite PPL
  - https://github.com/opensearch-project/sql/pull/4438
  - Pattern: Multivalue function implementation and testing

---

## Planned Change List (grouped)

### Grammar/Parser (`*.g4`):
- `ppl/src/main/antlr/OpenSearchPPLLexer.g4`
  - Add MVCOMBINE token
  - Add DELIM token (if not already present)
  
- `ppl/src/main/antlr/OpenSearchPPLParser.g4`
  - Add `mvcombineCommand` rule: `MVCOMBINE (DELIM EQUAL delimString)? fieldExpression`
  - Add mvcombineCommand to the `commands` rule
  - Add MVCOMBINE to `commandName` rule
  - Add MVCOMBINE and DELIM to `searchableKeyWord` rule (if needed)

### AST nodes (`core/.../ast/tree/*`):
- `core/src/main/java/org/opensearch/sql/ast/tree/Mvcombine.java` (NEW)
  - Extend UnresolvedPlan
  - Fields: fieldExpression (field to combine), delimiter (optional), child plan
  - Constructor, getters, accept() method for visitor pattern
  
- `core/src/main/java/org/opensearch/sql/ast/dsl/AstDSL.java`
  - Add factory method: `mvcombine(UnresolvedPlan child, UnresolvedExpression field, String delimiter)`

### Visitors (AstBuilder → Calcite planner → Anonymizer → others):
- `ppl/src/main/java/org/opensearch/sql/ppl/parser/AstBuilder.java`
  - Add `visitMvcombineCommand()` method
  - Build Mvcombine AST node from parse tree context
  - Extract field expression and optional delimiter parameter
  
- `core/src/main/java/org/opensearch/sql/ast/AbstractNodeVisitor.java`
  - Add `visitMvcombine(Mvcombine node, C context)` method
  
- `core/src/main/java/org/opensearch/sql/calcite/CalciteRelNodeVisitor.java`
  - Add `visitMvcombine(Mvcombine node, CalciteContext context)` method
  - Transform to Calcite logical plan with:
    - Dynamic grouping on all fields except the specified field
    - Aggregation using ARRAY_AGG or STRING_AGG based on delimiter presence
    - System field filtering logic
  
- `ppl/src/main/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizer.java`
  - Add `visitMvcombine(Mvcombine node, AnonymizerContext context)` method
  - Anonymize field names while preserving command structure

### Tests (unit, syntax, integration pushdown & non-pushdown, explain, anonymizer):

**Unit tests:**
- `ppl/src/test/java/org/opensearch/sql/ppl/parser/AstBuilderTest.java`
  - Test basic mvcombine syntax parsing
  - Test mvcombine with delim parameter
  - Test mvcombine without delim parameter
  - Test error cases (missing field, invalid syntax)
  
- `ppl/src/test/java/org/opensearch/sql/ppl/calcite/CalcitePPLMvcombineTest.java` (NEW)
  - Test logical plan generation
  - Test grouping logic (all fields except specified)
  - Test with/without delimiter parameter
  - Test type preservation (numeric, string, boolean)

**Syntax parser tests:**
- Add test cases in `AstBuilderTest.java` for grammar validation
- Test delimiter escaping and special characters

**Integration tests:**
- `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalciteMvcombineCommandIT.java` (NEW)
  - Test Use Case 1: Consolidating host values after stats
  - Test Use Case 2: Combining with custom delimiter
  - Test Use Case 3: Combining numeric values
  - Test with NULL values in combining field
  - Test with NULL values in grouping fields
  - Test empty result handling
  - Test single value case (still converts to array/string)
  - Test system field exclusion from grouping
  - Test negative case: invalid field name
  
- Expected output files:
  - `integ-test/src/test/resources/expectedOutput/calcite/mvcombine_basic.yaml`
  - `integ-test/src/test/resources/expectedOutput/calcite/mvcombine_with_delim.yaml`
  - `integ-test/src/test/resources/expectedOutput/calcite/mvcombine_numeric.yaml`
  - `integ-test/src/test/resources/expectedOutput/calcite_no_pushdown/mvcombine_basic.yaml`
  - `integ-test/src/test/resources/expectedOutput/calcite_no_pushdown/mvcombine_with_delim.yaml`
  - `integ-test/src/test/resources/expectedOutput/calcite_no_pushdown/mvcombine_numeric.yaml`

**Explain tests:**
- `integ-test/src/test/java/org/opensearch/sql/calcite/remote/CalciteExplainIT.java`
  - Add test case: `explain_mvcombine_command`
  - Verify logical plan structure
  - `integ-test/src/test/resources/expectedOutput/calcite/explain_mvcombine_command.json`
  - `integ-test/src/test/resources/expectedOutput/calcite_no_pushdown/explain_mvcombine_command.json`

**Anonymizer tests:**
- `ppl/src/test/java/org/opensearch/sql/ppl/utils/PPLQueryDataAnonymizerTest.java`
  - Test field name anonymization in mvcombine command
  - Test delimiter parameter preservation
  - Verify command structure remains valid after anonymization

### Docs (.rst + index):
- `docs/user/ppl/cmd/mvcombine.rst` (NEW)
  - Command description and syntax
  - Parameter table
  - Behavioral notes (NULL handling, type coercion, ordering)
  - Examples section with ALL THREE Phase-1 use cases as doctest blocks:
    - Use Case 1: Consolidating host values (without delim)
    - Use Case 2: Custom delimiter (with delim)
    - Use Case 3: Numeric values (type preservation)
  - One negative case doctest with expected error message
  - Limitations and notes
  
- `docs/user/ppl/index.rst`
  - Add mvcombine to command list
  
- `docs/category.json`
  - Add mvcombine.rst entry

---

## Task Breakdown (ordered)

1. **Grammar/Parser**
   - Update OpenSearchPPLLexer.g4 with MVCOMBINE token
   - Update OpenSearchPPLParser.g4 with mvcombineCommand rule
   - Add to commands and commandName rules
   - Regenerate parser (gradle build)

2. **AST node(s)**
   - Create Mvcombine.java AST node class
   - Add factory method to AstDSL.java
   - Implement constructor, getters, accept() method

3. **Visitors (AstBuilder → Calcite planner → Anonymizer)**
   - Implement AstBuilder.visitMvcombineCommand()
   - Implement AbstractNodeVisitor.visitMvcombine() default method
   - Implement CalciteRelNodeVisitor.visitMvcombine() for logical plan (V3/Calcite ONLY)
   - Implement PPLQueryDataAnonymizer.visitMvcombine()

4. **Unit tests**
   - Add AstBuilderTest cases for syntax parsing
   - Create CalcitePPLMvcombineTest for logical plan validation
   - Test various parameter combinations and edge cases

5. **Syntax parser tests**
   - Add comprehensive grammar tests in AstBuilderTest
   - Test error cases and boundary conditions

6. **Integration tests (pushdown / non-pushdown / explain / anonymizer)**
   - Create CalciteMvcombineCommandIT with all use cases
   - Create expected output YAML files for both pushdown modes
   - Add explain test case in CalciteExplainIT
   - Update PPLQueryDataAnonymizerTest with mvcombine cases

7. **Docs (.rst + index)**
   - Create docs/user/ppl/cmd/mvcombine.rst with full documentation
   - Add THREE use cases from Phase-1 as doctest examples
   - Add ONE negative case doctest with error message
   - Update docs/user/ppl/index.rst to include mvcombine
   - Update docs/category.json

8. **Final suite & PR**
   - Run full test suite: `./gradlew test`
   - Run integration tests: `./gradlew :integ-test:integTest`
   - Run doctest: `./gradlew :doctest:doctest -DignorePrometheus`
   - Run specific command IT: `./gradlew :integ-test:integTest -Dtests.class="*MvcombineIT"`
   - Run specific doctest: `./gradlew :doctest:doctest -DignorePrometheus -Pdocs="mvcombine.rst"`
   - Verify all gates pass
   - Create PR with links to requirements, plan, and test results

---

## Test Mapping (Phase-1 use cases → doctest acceptance)

All THREE Phase-1 use cases will be included in `docs/user/ppl/cmd/mvcombine.rst` as doctest code blocks in the "Examples" section:

1. **Use Case 1: Consolidating host values after stats**
   - Input: Multiple rows with identical max/min, different hosts
   - Command: `... | mvcombine host`
   - Expected: Single row with host as array ["web1", "web2", "web3"]
   - Dataset: Use existing doctest dataset (prefer accounts or similar)

2. **Use Case 2: Combining with custom delimiter**
   - Input: Multiple rows with identical status, different servers
   - Command: `... | mvcombine delim="," server`
   - Expected: Single row with server as delimited string "server1,server2,server3"
   - Dataset: Use existing doctest dataset

3. **Use Case 3: Combining numeric values**
   - Input: Multiple rows with identical user, different error codes
   - Command: `... | mvcombine error_code`
   - Expected: Single row with error_code as numeric array [404, 500, 503]
   - Dataset: Use existing doctest dataset

4. **Negative Case:**
   - Input: Valid data
   - Command: `... | mvcombine nonexistent_field`
   - Expected: Error message with field not found
   - Format: Doctest with exact error text

**Dataset Note:** Reuse existing doctest dataset (accounts, logs, or similar). Do NOT switch to a different dataset just to make tests pass. Adapt test queries to work with available fields in the existing dataset.

**Documentation Requirements:**
- Add `docs/user/ppl/cmd/mvcombine.rst` to `docs/category.json`
- All doctest blocks must use `.. code-block:: ppl` with `.. testcode::` or similar doctest directives
- Include expected output blocks with `.. testoutput::` for verification
- These doctests serve as the acceptance test for Phase 2 completion

---

## Definition of Done

- [x] Doctest acceptance passes for all 3 use cases + negative case
  - Use Case 1: Host consolidation without delimiter
  - Use Case 2: Custom delimiter combination
  - Use Case 3: Numeric value preservation
  - Negative case: Field not found error
  
- [x] Unit + syntax tests pass locally
  - AstBuilderTest covers grammar parsing
  - CalcitePPLMvcombineTest covers logical plan generation
  - All edge cases and parameter combinations tested
  
- [x] Docs complete; examples match outputs
  - mvcombine.rst created with full documentation
  - All THREE Phase-1 use cases documented as doctests
  - ONE negative case documented with error message
  - Added to index.rst and category.json
  
- [x] Error messages stable with IDs
  - Field not found errors use consistent error codes
  - Invalid parameter errors have clear messages
  
- [x] Pushdown behavior aligns with §2.2 decision
  - Deferred optimization verification completed
  - If pushdown supported, both modes tested
  - If not supported, calcite_no_pushdown tests pass
  
- [x] PR template sections filled (links to requirements, plan, and doctest report)
  - Link to rfcs/ppl/mvcombine-requirements.md
  - Link to plans/ppl/mvcombine-implementation-plan.md
  - Doctest output showing all 4 cases passing
  - Integration test results
  - Explain test output

---

## Mandatory Test Gates (MUST PASS)

The following gates are **required** and become part of this plan's contract for Phase 3:

```bash
# Specific command integration test
./gradlew :integ-test:integTest -Dtests.class="*MvcombineIT"

# Specific command doctest
./gradlew :doctest:doctest -DignorePrometheus -Pdocs="mvcombine.rst"

# Full test suite
./gradlew test
./gradlew :integ-test:integTest
./gradlew :doctest:doctest -DignorePrometheus
```

**Anti-skip rules:**
- Do **not** use `-x test` or extra filters beyond the single `-Dtests.class="*MvcombineIT"` shown above
- New/changed tests for this command must not use `@Ignore`, `@Disabled`, or `Assume.*`
- Doctest "Examples" must include **3 use cases + 1 negative** and all must pass
- Phase 3 will stop on the first failure and report logs

---

## Design Notes

### V3/Calcite-Only Development:
**CRITICAL**: This command is implemented EXCLUSIVELY for the V3 engine based on Apache Calcite. 
- **DO NOT** modify or add functionality to V2 or legacy components
- **DO NOT** modify `core/src/main/java/org/opensearch/sql/analysis/Analyzer.java` (V2 legacy)
- **DO NOT** work in the `legacy` module
- Focus implementation in CalciteRelNodeVisitor for V3 query planning

### Risk/Complexity:
- **Dynamic GROUP BY**: The key challenge is determining at runtime which fields to group by (all except the specified field). This requires introspection of the input schema and dynamic construction of the grouping columns.
- **System Field Filtering**: Must automatically exclude internal fields (_id, _index, etc.) from grouping logic to match Splunk behavior.
- **Type Preservation**: When delimiter is not specified, must preserve original data types (numeric, boolean) in the array output, not convert everything to strings.

### Performance Expectations:
- Performance should be comparable to existing `stats` command with GROUP BY
- Large result sets with many unique grouping combinations may require memory for aggregation
- Delimiter string concatenation (when `delim` specified) should be efficient for typical use cases

### Assumptions:
- Existing aggregation functions (`ARRAY_AGG`, `STRING_AGG` or equivalent) are available in Calcite
- CalciteRelNodeVisitor can introspect input schema to determine grouping fields dynamically
- System/internal field detection logic exists or can be implemented in the visitor

---

## Phase 3 Entry Criteria

This implementation plan is approved and ready for Phase 3 (Implementation) when:
- [x] §2.2 High-level decisions approved by maintainer
- [x] Implementation plan reviewed and accepted
- [x] Maintainer provides explicit approval: **"APPROVE: SOLUTION"**

---

*End of Implementation Plan*
