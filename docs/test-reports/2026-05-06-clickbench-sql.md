# ClickBench SQL Correctness Report

Generated: Wed May 06 17:44:54 CDT 2026

Cluster: `localhost:9200`

Routing: `tests.analytics.force_routing=true`

Query path: **analytics-engine (unified path)**

Snapshot mode: **WRITE** (capturing)

## Summary

| Metric | Value |
|---|---:|
| Total queries | 43 |
| Passed | 0 |
| Failed | 10 |
| No reference | 0 |
| Snapshots written | 33 |
| Pass rate | **0.0%** (of 10 comparable) |
| Total time | 2.0s |

## Per-query results

| # | Status | SQL | Test Results |
|---:|:--|---|---|
| 1 | ✓ | SELECT COUNT(*) FROM hits | 1 row(s) returned |
| 2 | ✓ | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 | 1 row(s) returned |
| 3 | ✓ | SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits | 1 row(s) returned |
| 4 | ✓ | SELECT AVG(UserID) FROM hits | 1 row(s) returned |
| 5 | ✓ | SELECT COUNT(DISTINCT UserID) FROM hits | 1 row(s) returned |
| 6 | ✓ | SELECT COUNT(DISTINCT SearchPhrase) FROM hits | 1 row(s) returned |
| 7 | ✓ | SELECT MIN(EventDate), MAX(EventDate) FROM hits | 1 row(s) returned |
| 8 | ✓ | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*… | 1 row(s) returned |
| 9 | ✓ | SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10 | 2 row(s) returned |
| 10 | ✓ | SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM… | 2 row(s) returned |
| 11 | ✓ | SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP B… | 1 row(s) returned |
| 12 | ✓ | SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel … | 1 row(s) returned |
| 13 | ✓ | SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER B… | 1 row(s) returned |
| 14 | ✓ | SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY Search… | 1 row(s) returned |
| 15 | ✓ | SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY Sear… | 1 row(s) returned |
| 16 | ✓ | SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 | 2 row(s) returned |
| 17 | ✓ | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DES… | 2 row(s) returned |
| 18 | ✓ | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10 | 2 row(s) returned |
| 19 | ✗ | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserI… | No backend supports scalar function [EXTRACT] among [datafusion] |
| 20 | ✓ | SELECT UserID FROM hits WHERE UserID = 435090932899640449 | 1 row(s) returned |
| 21 | ✓ | SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' | 1 row(s) returned |
| 22 | ✗ | SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <… | Unable to find binding for call MIN($1) |
| 23 | ✗ | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Ti… | Unable to find binding for call MIN($1) |
| 24 | ✓ | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 | 1 row(s) returned |
| 25 | ✓ | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10 | 1 row(s) returned |
| 26 | ✓ | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 | 1 row(s) returned |
| 27 | ✓ | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10 | 1 row(s) returned |
| 28 | ✗ | SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID… | No backend supports scalar function [null] among [datafusion] |
| 29 | ✗ | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) … | No enum constant org.opensearch.analytics.spi.ScalarFunction.REGEXP_REPLACE |
| 30 | ✓ | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidt… | 1 row(s) returned |
| 31 | ✓ | SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHER… | 1 row(s) returned |
| 32 | ✓ | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE Searc… | 1 row(s) returned |
| 33 | ✓ | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY Wa… | 2 row(s) returned |
| 34 | ✓ | SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10 | 2 row(s) returned |
| 35 | ✓ | SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 | 2 row(s) returned |
| 36 | ✓ | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientI… | 2 row(s) returned |
| 37 | ✓ | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND … | 1 row(s) returned |
| 38 | ✓ | SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AN… | 1 row(s) returned |
| 39 | ✗ | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND … | empty result set (0 rows) — test data may not satisfy query filters |
| 40 | ✗ | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID =… | No backend supports scalar function [CASE] among [datafusion] |
| 41 | ✗ | SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2… | empty result set (0 rows) — test data may not satisfy query filters |
| 42 | ✗ | SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 … | empty result set (0 rows) — test data may not satisfy query filters |
| 43 | ✗ | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 A… | No enum constant org.opensearch.analytics.spi.ScalarFunction.DATE_TRUNC |

## Errors

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

### q39

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
empty result set (0 rows) — test data may not satisfy query filters
```

### q40

```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
No backend supports scalar function [CASE] among [datafusion]
```

### q41

```sql
SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100
```

```
empty result set (0 rows) — test data may not satisfy query filters
```

### q42

```sql
SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
```

```
empty result set (0 rows) — test data may not satisfy query filters
```

### q43

```sql
SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000
```

```
No enum constant org.opensearch.analytics.spi.ScalarFunction.DATE_TRUNC
```

