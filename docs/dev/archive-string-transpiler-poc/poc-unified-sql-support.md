# PoC: SQL and ANSI SQL Support in Unified Query API

## Objective

Add two new language types to the unified query API alongside the existing PPL support:

- **`SQL`** — OpenSearch SQL dialect. Uses our ANTLR-based SQL parser to produce the shared AST (`UnresolvedPlan`), then follows the same CalciteRelNodeVisitor path as PPL to produce a Calcite `RelNode`.
- **`ANSI_SQL`** — Standard ANSI SQL. Bypasses our AST layer entirely and uses Calcite's built-in `SqlParser` → `SqlNode` → `SqlToRelConverter` → `RelNode` pipeline.

All three language paths converge at `RelNode`, so `UnifiedQueryCompiler` and `UnifiedQueryTranspiler` work unchanged downstream.

---

## Architecture: Three Pipelines

```
PPL path:
  query → PPLSyntaxParser → ANTLR ParseTree → PPL AstBuilder → UnresolvedPlan AST
        → CalciteRelNodeVisitor → RelNode

SQL path:
  query → SQLSyntaxParser → ANTLR ParseTree → SQL AstBuilder → UnresolvedPlan AST
        → CalciteRelNodeVisitor → RelNode

ANSI_SQL path:
  query → Calcite SqlParser → SqlNode → SqlValidator → SqlToRelConverter → RelNode
```

The SQL path reuses the same visitor-based analysis as PPL because both produce the same shared AST node types (`Project`, `Filter`, `Relation`, `Sort`, `Aggregation`, etc.). The ANSI_SQL path is completely independent — it delegates everything to Calcite's own SQL infrastructure.

---

## Current State (PPL Only)

The unified query API currently lives in the `api/` module. The flow is:

1. `UnifiedQueryContext` — Central config: catalog schemas, query type, settings, Calcite `FrameworkConfig`.
2. `UnifiedQueryPlanner` — Parses query string → AST → `CalciteRelNodeVisitor` → `RelNode`.
3. `UnifiedQueryCompiler` — Compiles `RelNode` → JDBC `PreparedStatement`.
4. `UnifiedQueryTranspiler` — Converts `RelNode` back to SQL string via `RelToSqlConverter`.

The bottleneck is in `UnifiedQueryPlanner`, which has two PPL-specific hardcodings:
- `buildQueryParser()` only returns `PPLSyntaxParser`
- `parse()` uses PPL-specific `AstBuilder` and `AstStatementBuilder`

Everything downstream is already language-agnostic.

### Key Enablers

- The SQL module (`sql/`) already has the same parser chain as PPL:
  - `SQLSyntaxParser` implements the same `org.opensearch.sql.common.antlr.Parser` interface
  - SQL's `AstBuilder` produces the same `UnresolvedPlan` AST nodes
  - SQL's `AstStatementBuilder` wraps plans in `Query` statements
- `QueryType.SQL` already exists in the enum (just needs `ANSI_SQL` added)
- `CalciteRelNodeVisitor` already handles most AST nodes that SQL produces
- `UnifiedQueryContext.buildFrameworkConfig()` already sets `SqlParser.Config.DEFAULT` and registers catalog schemas — this is exactly what Calcite's `Planner` API needs
- `CalciteToolsHelper` already has `OpenSearchSqlToRelConverter` wired up for Calcite's internal SQL→Rel conversion

---

## File-by-File Changes

### 1. `core/src/main/java/org/opensearch/sql/executor/QueryType.java`

Add `ANSI_SQL`:

```java
public enum QueryType {
  PPL,
  SQL,
  ANSI_SQL
}
```

### 2. `api/build.gradle`

Add `sql` module dependency. Currently only depends on `ppl`:

```groovy
dependencies {
    api project(':ppl')
    api project(':sql')   // NEW
    // ... rest unchanged
}
```

The `ppl` module transitively brings in `core` and `common`. The `sql` module depends on `core` and `common` as well, so no diamond dependency issues.

### 3. `api/src/main/java/org/opensearch/sql/api/UnifiedQueryPlanner.java`

This is the main change. Three things to modify:

#### 3a. Refactor `plan()` to branch on query type

```java
public RelNode plan(String query) {
    try {
        return switch (context.getPlanContext().queryType) {
            case PPL, SQL -> preserveCollation(analyze(parse(query)));
            case ANSI_SQL -> planWithCalcite(query);
        };
    } catch (SyntaxCheckException e) {
        throw e;
    } catch (Exception e) {
        throw new IllegalStateException("Failed to plan query", e);
    }
}
```

#### 3b. Refactor `buildQueryParser()` for SQL

```java
private Parser buildQueryParser(QueryType queryType) {
    return switch (queryType) {
        case PPL -> new PPLSyntaxParser();
        case SQL -> new SQLSyntaxParser();
        case ANSI_SQL -> null; // not used — Calcite handles parsing directly
    };
}
```

#### 3c. Refactor `parse()` to dispatch to correct AST builders

The current `parse()` hardcodes PPL's `AstBuilder` and `AstStatementBuilder`. It needs to branch because:
- PPL's `AstBuilder` constructor: `AstBuilder(String query, Settings settings)`
- SQL's `AstBuilder` constructor: `AstBuilder(String query)` (no Settings param)
- They extend different ANTLR base visitors (`OpenSearchPPLParserBaseVisitor` vs `OpenSearchSQLParserBaseVisitor`)
- Their `AstStatementBuilder` classes have different `StatementBuilderContext` fields

```java
private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    Statement statement = switch (context.getPlanContext().queryType) {
        case PPL -> {
            var astBuilder = new org.opensearch.sql.ppl.parser.AstBuilder(
                query, context.getSettings());
            var stmtBuilder = new org.opensearch.sql.ppl.parser.AstStatementBuilder(
                astBuilder,
                org.opensearch.sql.ppl.parser.AstStatementBuilder
                    .StatementBuilderContext.builder().build());
            yield cst.accept(stmtBuilder);
        }
        case SQL -> {
            var astBuilder = new org.opensearch.sql.sql.parser.AstBuilder(query);
            var stmtBuilder = new org.opensearch.sql.sql.parser.AstStatementBuilder(
                astBuilder,
                org.opensearch.sql.sql.parser.AstStatementBuilder
                    .StatementBuilderContext.builder().build());
            yield cst.accept(stmtBuilder);
        }
        default -> throw new IllegalArgumentException(
            "Unsupported query type for AST parsing: "
                + context.getPlanContext().queryType);
    };

    if (statement instanceof Query) {
        return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got "
            + statement.getClass().getSimpleName());
}
```

#### 3d. New method for ANSI_SQL path

Uses Calcite's `Frameworks.getPlanner()` API with the same `FrameworkConfig` that the AST-based paths use (same schemas, same catalog registrations):

```java
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

private RelNode planWithCalcite(String query) {
    try {
        Planner planner = Frameworks.getPlanner(context.getPlanContext().config);
        SqlNode parsed = planner.parse(query);
        SqlNode validated = planner.validate(parsed);
        RelRoot relRoot = planner.rel(validated);
        planner.close();
        return relRoot.rel;
    } catch (Exception e) {
        throw new IllegalStateException("Failed to plan ANSI SQL query", e);
    }
}
```

**Why this works**: `UnifiedQueryContext.buildFrameworkConfig()` already constructs a `FrameworkConfig` with:
- All registered catalog schemas via `rootSchema.add(name, schema)`
- Default schema resolution via `defaultSchema`
- `SqlParser.Config.DEFAULT` as the parser config
- Calcite programs and trait definitions

The `Planner` created from this config will resolve table names against the same schema hierarchy.

#### 3e. Constructor adjustment

The constructor currently eagerly creates a `Parser`. For `ANSI_SQL`, the parser is null since Calcite handles parsing. Guard accordingly:

```java
public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.parser = buildQueryParser(context.getPlanContext().queryType);
    this.context = context;
}
```

This is fine as-is — `parser` will be null for `ANSI_SQL`, and `parse()` is never called for that path since `plan()` dispatches to `planWithCalcite()` instead.

