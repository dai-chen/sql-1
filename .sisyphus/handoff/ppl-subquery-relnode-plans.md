# PPL Subquery RelNode Plans — Correlated Shape Evidence

All examples from unit tests in `ppl/src/test/java/org/opensearch/sql/ppl/calcite/`.

---

## 1. IN Subquery (single column)

**File:** `CalcitePPLInSubqueryTest.java:24` — `testInSubquery()`

```ppl
source=EMP | where DEPTNO in [ source=DEPT | fields DEPTNO ]
| sort - EMPNO | fields EMPNO, ENAME
```

**Logical Plan:**
```
LogicalProject(EMPNO=[$0], ENAME=[$1])
  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])
    LogicalFilter(condition=[IN($7, {
LogicalProject(DEPTNO=[$0])
  LogicalTableScan(table=[[scott, DEPT]])
})], variablesSet=[[$cor0]])
      LogicalTableScan(table=[[scott, EMP]])
```

**Correlated shape:** ✅ `variablesSet=[[$cor0]]` present on `LogicalFilter` with `IN(...)` containing a `RexSubQuery`.

---

## 2. NOT IN Subquery (multi-column)

**File:** `CalcitePPLInSubqueryTest.java:144` — `testNotInSubquery()`

```ppl
source=EMP | where (DEPTNO, ENAME) not in [ source=DEPT | fields DEPTNO, DNAME ]
| sort - EMPNO | fields EMPNO, ENAME
```

**Logical Plan:**
```
LogicalProject(EMPNO=[$0], ENAME=[$1])
  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])
    LogicalFilter(condition=[NOT(IN($7, $1, {
LogicalProject(DEPTNO=[$0], DNAME=[$1])
  LogicalTableScan(table=[[scott, DEPT]])
}))], variablesSet=[[$cor0]])
      LogicalTableScan(table=[[scott, EMP]])
```

**Correlated shape:** ✅ `variablesSet=[[$cor0]]` on `LogicalFilter` with `NOT(IN(...))`.

---

## 3. EXISTS Subquery (correlated)

**File:** `CalcitePPLExistsSubqueryTest.java:21` — `testCorrelatedExistsSubqueryWithoutAlias()`

```ppl
source=EMP
| where exists [
    source=SALGRADE
    | where SAL = HISAL
  ]
| sort - EMPNO | fields EMPNO, ENAME
```

**Logical Plan:**
```
LogicalProject(EMPNO=[$0], ENAME=[$1])
  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])
    LogicalFilter(condition=[EXISTS({
LogicalFilter(condition=[=($cor0.SAL, $2)])
  LogicalTableScan(table=[[scott, SALGRADE]])
})], variablesSet=[[$cor0]])
      LogicalTableScan(table=[[scott, EMP]])
```

**Correlated shape:** ✅ `variablesSet=[[$cor0]]` on `LogicalFilter` with `EXISTS(...)`. Inner plan references `$cor0.SAL`.

---

## 4. Scalar Subquery (correlated, in WHERE)

**File:** `CalcitePPLScalarSubqueryTest.java:119` — `testCorrelatedScalarSubqueryInWhere()`

```ppl
source=EMP
| where SAL > [
    source=SALGRADE | where SAL = HISAL | stats AVG(SAL)
  ]
```

**Logical Plan:**
```
LogicalFilter(condition=[>($5, $SCALAR_QUERY({
LogicalAggregate(group=[{}], AVG(SAL)=[AVG($0)])
  LogicalProject($f0=[$cor0.SAL])
    LogicalFilter(condition=[=($cor0.SAL, $2)])
      LogicalTableScan(table=[[scott, SALGRADE]])
}))], variablesSet=[[$cor0]])
  LogicalTableScan(table=[[scott, EMP]])
```

**Correlated shape:** ✅ `variablesSet=[[$cor0]]` on `LogicalFilter` with `$SCALAR_QUERY(...)`. Inner plan references `$cor0.SAL`.

---

## Comparison with PR #5448 SQL V2 IN-subquery Plan

PR #5448's SQL test produces:
```
LogicalFilter(condition=[IN($2, {...})], variablesSet=[[$cor0]])
```

**Conclusion:** The PPL plans above produce the **identical correlated RelNode shape**:
- `LogicalFilter` with `variablesSet=[[$cor0]]`
- Condition contains `RexSubQuery` nodes (`IN(...)`, `NOT(IN(...))`, `EXISTS(...)`, `$SCALAR_QUERY(...)`)
- The subquery body is an inline `RelNode` tree inside the condition

This is the same shape that Calcite's `SubQueryRemoveRule` / decorrelation rules handle in the Adaptive Engine (AE). PPL already relies on AE decorrelation for all three subquery types — the SQL V2 IN-subquery in PR #5448 follows the exact same pattern.
