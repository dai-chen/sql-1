# ClickBench SQL — Analytics Engine Compatibility Report

Generated: 2026-05-04 23:53

## Configuration

- **Cluster**: OpenSearch 3.7.0-SNAPSHOT with analytics-engine plugin set
- **Plugins**: analytics-engine, analytics-backend-datafusion, analytics-backend-lucene, composite-engine, parquet-data-format, opensearch-sql
- **Index settings**: `pluggable.dataformat=composite`, `composite.primary_data_format=parquet`, 1 shard
- **Routing**: `plugins.calcite.analytics.force_routing=true` (all queries forced through analytics-engine path)
- **SQL branch**: `feature/mustang-sql-it-local-changes` (dai-chen/sql-1)

## Summary

| Metric | Value |
|---|---:|
| Queries run | 43 |
| Passed | 28 |
| Failed | 15 |
| Pass rate | **65.1%** |

## Per-query Results

| # | Status | SQL | Detail |
|---:|:--|---|---|
| 1 | ✓ PASS | SELECT COUNT(*) FROM hits | 1 rows |
| 2 | ✓ PASS | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 | 1 rows |
| 3 | ✓ PASS | SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits | 1 rows |
| 4 | ✓ PASS | SELECT AVG(UserID) FROM hits | 1 rows |
| 5 | ✓ PASS | SELECT COUNT(DISTINCT UserID) FROM hits | 1 rows |
| 6 | ✓ PASS | SELECT COUNT(DISTINCT SearchPhrase) FROM hits | 1 rows |
| 7 | ✓ PASS | SELECT MIN(EventDate), MAX(EventDate) FROM hits | 1 rows |
| 8 | ✓ PASS | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) | 1 rows |
| 9 | ✓ PASS | SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10 | 2 rows |
| 10 | ✓ PASS | SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM  | 2 rows |
| 11 | ✓ PASS | SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY | 1 rows |
| 12 | ✓ PASS | SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel < | 1 rows |
| 13 | ✓ PASS | SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY | 1 rows |
| 14 | ✓ PASS | SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchP | 1 rows |
| 15 | ✓ PASS | SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY Searc | 1 rows |
| 16 | ✓ PASS | SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 | 2 rows |
| 17 | ✓ PASS | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC | 2 rows |
| 18 | ✓ PASS | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10 | 2 rows |
| 19 | ✗ FAIL | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID | No backend supports scalar function [EXTRACT] among [datafusion] |
| 20 | ✓ PASS | SELECT UserID FROM hits WHERE UserID = 435090932899640449 | 1 rows |
| 21 | ✓ PASS | SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' | 1 rows |
| 22 | ✗ FAIL | SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> | Unable to find binding for call MIN($1) |
| 23 | ✗ FAIL | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Tit | Unable to find binding for call MIN($1) |
| 24 | ✓ PASS | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 | 1 rows |
| 25 | ✓ PASS | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10 | 1 rows |
| 26 | ✓ PASS | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 | 1 rows |
| 27 | ✓ PASS | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10 | 1 rows |
| 28 | ✗ FAIL | SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID  | No backend supports scalar function [null] among [datafusion] |
| 29 | ✗ FAIL | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) A | No enum constant org.opensearch.analytics.spi.ScalarFunction.REGEXP_REPLACE |
| 30 | ✗ FAIL | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM hits | Failed to plan query: [SqlParseException] Encountered ", .." at line 1, column 54.
Was expecting one |
| 31 | ✓ PASS | SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE | 1 rows |
| 32 | ✓ PASS | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE Search | 1 rows |
| 33 | ✓ PASS | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY Wat | 2 rows |
| 34 | ✓ PASS | SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10 | 2 rows |
| 35 | ✗ FAIL | SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 | Unrecognized aggregate function [ANY_VALUE] |
| 36 | ✗ FAIL | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP | No backend supports scalar function [MINUS] among [datafusion] |
| 37 | ✗ FAIL | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND E | Unrecognized filter operator [SEARCH] |
| 38 | ✗ FAIL | SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND | Unrecognized filter operator [SEARCH] |
| 39 | ✗ FAIL | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND E | Unrecognized filter operator [SEARCH] |
| 40 | ✗ FAIL | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID =  | Failed to plan query: [ValidationException] java.lang.NullPointerException: Cannot invoke "org.apach |
| 41 | ✗ FAIL | SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '20 | Unrecognized filter operator [SEARCH] |
| 42 | ✗ FAIL | SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 A | Unrecognized filter operator [SEARCH] |
| 43 | ✗ FAIL | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AN | Failed to plan query: [ValidationException] org.apache.calcite.runtime.CalciteContextException: From |

## Failure Analysis

| Root Cause | Queries | Count |
|---|---|---:|
| Unrecognized filter operator [SEARCH] (Calcite LIKE→SEARCH rewrite) | q37, q38, q39, q41, q42 | 5 |
| Unable to find binding for MIN on string column | q22, q23 | 2 |
| No backend supports scalar function [EXTRACT] | q19 | 1 |
| No backend supports scalar function [null] (length() not mapped) | q28 | 1 |
| No enum constant ScalarFunction.REGEXP_REPLACE | q29 | 1 |
| SqlParseException (test-asset bug: literal '...' in q30) | q30 | 1 |
| Unrecognized aggregate function [ANY_VALUE] (GROUP BY ordinal) | q35 | 1 |
| No backend supports scalar function [MINUS] | q36 | 1 |
| NPE in Calcite validator (CASE WHEN ... END AS alias) | q40 | 1 |
| DATE_TRUNC signature mismatch (CHAR, TIMESTAMP) | q43 | 1 |

## Failure Details

### q19

```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
```

```
No backend supports scalar function [EXTRACT] among [datafusion]
```

### q22

```sql
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

```
Unable to find binding for call MIN($1)
```

### q23

```sql
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

```
Unable to find binding for call MIN($1)
```

### q28

```sql
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

```
No backend supports scalar function [null] among [datafusion]
```

### q29

```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

```
No enum constant org.opensearch.analytics.spi.ScalarFunction.REGEXP_REPLACE
```

### q30

```sql
SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), ... SUM(ResolutionWidth + 89) FROM hits
```

```
Failed to plan query: [SqlParseException] Encountered ", .." at line 1, column 54.
Was expecting one of:
    <EOF> 
    "ORDER" ...
    "LIMIT" ...
    "OFFSET" ...
    "FETCH" ...
    "," "*" ...
   
```

### q35

```sql
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10
```

```
Unrecognized aggregate function [ANY_VALUE]
```

### q36

```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10
```

```
No backend supports scalar function [MINUS] among [datafusion]
```

### q37

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10
```

```
Unrecognized filter operator [SEARCH]
```

### q38

```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10
```

```
Unrecognized filter operator [SEARCH]
```

### q39

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
Unrecognized filter operator [SEARCH]
```

### q40

```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
Failed to plan query: [ValidationException] java.lang.NullPointerException: Cannot invoke "org.apache.calcite.sql.SqlNode.accept(org.apache.calcite.sql.util.SqlVisitor)" because "node" is null
```

### q41

```sql
SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100
```

```
Unrecognized filter operator [SEARCH]
```

### q42

```sql
SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
```

```
Unrecognized filter operator [SEARCH]
```

### q43

```sql
SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000
```

```
Failed to plan query: [ValidationException] org.apache.calcite.runtime.CalciteContextException: From line 1, column 208 to line 1, column 238: Cannot apply 'DATE_TRUNC' to arguments of type 'DATE_TRUN
```

