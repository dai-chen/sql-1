# Verification: V2 Alternate Syntax Claims

## A. ACTUAL TEST SYNTAX (Claim 1)

**File:** `integ-test/src/test/java/org/opensearch/sql/sql/MultiMatchIT.java`

### `multi_match_alternate_syntax` (line 144):
```sql
SELECT Id FROM <TEST_INDEX_BEER> WHERE CreationDate = multi_match('2014-01-22');
```

### `multimatch_alternate_syntax` (line 152):
```sql
SELECT Id FROM <TEST_INDEX_BEER> WHERE CreationDate = multimatch('2014-01-22');
```

**Assessment:** The syntax is `field = multi_match('value')` — a relevance function on the RHS of `=`. The claim is **confirmed**, though the Quip doc's paraphrase `(field = multi_match('value'))` adds parentheses that aren't in the actual SQL.

---

## B. V2 LEGACY SUPPORT (Claim 2)

### Grammar Rule
**File:** `sql/src/main/antlr/OpenSearchSQLParser.g4`, lines 485-487:
```antlr
altMultiFieldRelevanceFunction
   : field = relevanceField EQUAL_SYMBOL altSyntaxFunctionName = altMultiFieldRelevanceFunctionName LR_BRACKET query = relevanceQuery (COMMA relevanceArg)* RR_BRACKET
   ;
```

**File:** `sql/src/main/antlr/OpenSearchSQLParser.g4`, lines 727-730:
```antlr
altMultiFieldRelevanceFunctionName
   : MULTI_MATCH
   | MULTIMATCH
   ;
```

The `=` is NOT a comparison operator — it's part of the `altMultiFieldRelevanceFunction` grammar rule itself. The entire `CreationDate = multi_match('2014-01-22')` is parsed as a single `relevanceFunction` → `functionCall` → `expressionAtom` → `predicate`.

### AST Builder
**File:** `sql/src/main/java/org/opensearch/sql/sql/parser/AstExpressionBuilder.java`, lines 464-469:
```java
public UnresolvedExpression visitAltMultiFieldRelevanceFunction(
    AltMultiFieldRelevanceFunctionContext ctx) {
  return new Function(
      ctx.altSyntaxFunctionName.getText().toLowerCase(Locale.ROOT),
      altMultiFieldRelevanceFunctionArguments(ctx));
}
```

The AST builder converts this into a `Function("multi_match", [RelevanceFieldList({"CreationDate": 1.0}), Literal("2014-01-22")])` — NOT a comparison node. The field becomes a named argument.

**Conclusion:** V2 supports `field = multi_match(...)` natively via a dedicated grammar rule (`altMultiFieldRelevanceFunction`). It's syntactic sugar that the parser recognizes and the AST builder converts into a standard relevance function call with the field extracted as an argument. This is V2-specific syntax — it was designed specifically for the OpenSearch SQL grammar.

---

## C. CALCITE / NEW ENGINE BEHAVIOR (Claim 3)

### Key Finding: Calcite is NOT enabled for SQL queries

**File:** `core/src/main/java/org/opensearch/sql/executor/QueryService.java`, lines 360-364:
```java
// TODO https://github.com/opensearch-project/sql/issues/3457
// Calcite is not available for SQL query now. Maybe release in 3.1.0?
private boolean shouldUseCalcite(QueryType queryType) {
    return isCalciteEnabled(settings) && queryType == QueryType.PPL;
}
```

**SQL queries always use the V2 engine**, regardless of the Calcite setting. Calcite is only used for PPL queries.

### If Calcite were used for SQL (future state):

Two possible paths exist:

1. **Custom Visitor Strategy** (`api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java`, line 107-112): Uses the same ANTLR parser + AST builder → `CalciteRelNodeVisitor`. This path WOULD support the alternate syntax because:
   - The same grammar parses it into a `Function` AST node
   - `CalciteRexNodeVisitor.visitFunction` (line 497) resolves functions via `PPLFuncImpTable`
   - `multi_match` IS registered (`PPLFuncImpTable.java`, line 946)
   - `visitRelevanceFieldList` (line 750) converts field lists to MAP_VALUE_CONSTRUCTOR
   - `visitUnresolvedArgument` (line 771) wraps named args as MAP entries
   - `RelevanceQueryFunction` (`core/src/main/java/org/opensearch/sql/expression/function/udf/RelevanceQueryFunction.java`) accepts MAP-typed operands and returns BOOLEAN

2. **Calcite Native Parser** (`api/src/main/java/org/opensearch/sql/api/parser/CalciteSqlQueryParser.java`): Uses Calcite's own SQL parser. This would NOT recognize `field = multi_match(...)` as a relevance function — it would try to parse it as a standard comparison, and `multi_match` is not a standard SQL function in Calcite's parser.

### No explicit rejection found

There is NO explicit `CalciteUnsupportedException` thrown for relevance functions in comparison contexts. The `CalciteRexNodeVisitor` has `visitRelevanceFieldList` implemented (unlike `visitHighlightFunction` and `visitScoreFunction` which throw unsupported exceptions).

---

## D. "Not Valid SQL" Check

`field = function_call(...)` is syntactically valid ANSI SQL if `function_call` returns a comparable type. However:

- In this grammar, `CreationDate = multi_match('2014-01-22')` is NOT parsed as a comparison at all — it's a dedicated grammar rule
- Semantically, `multi_match` returns a boolean (relevance match), so `field = multi_match(value)` as a comparison would be type-nonsensical (comparing a date field to a boolean)
- The syntax only works because the grammar has a special rule that treats `=` as part of the function call syntax, not as a comparison operator

**Conclusion:** The claim "not valid SQL" is **partially correct**. It's not valid ANSI SQL because no standard SQL engine would interpret `field = multi_match(...)` as anything other than a comparison. It's valid only in OpenSearch SQL's extended grammar where it's syntactic sugar for `multi_match([field], value)`.

---

## E. FINAL VERDICT

| Claim | Verdict | Justification |
|-------|---------|---------------|
| 1. Tests use `field = multi_match('value')` | **Confirmed** | Exact SQL: `WHERE CreationDate = multi_match('2014-01-22')` and `WHERE CreationDate = multimatch('2014-01-22')` (`MultiMatchIT.java:145,153`) |
| 2. V2-specific syntax | **Confirmed** | Dedicated grammar rule `altMultiFieldRelevanceFunction` in `OpenSearchSQLParser.g4:485-487` with AST builder support in `AstExpressionBuilder.java:464-469` |
| 3. Calcite doesn't support it / not valid SQL | **Partially correct** | Calcite is currently NOT used for SQL queries at all (`QueryService.java:362-363`). If the Custom Visitor Strategy were used, it WOULD work since the same parser/AST is shared. If Calcite's native SQL parser were used, it would fail. The "not valid SQL" sub-claim is correct — it's OpenSearch-specific syntactic sugar, not standard SQL. |
