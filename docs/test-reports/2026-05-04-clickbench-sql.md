# ClickBench SQL Correctness Report

Generated: Tue May 05 06:22:40 GMT+03:00 2026

Cluster: `localhost:9200`

Routing: `tests.analytics.force_routing=true`

Query path: **analytics-engine (unified path)**

Snapshot mode: **WRITE** (capturing)

## Summary

| Metric | Value |
|---|---:|
| Total queries | 43 |
| Passed | 0 |
| Failed | 15 |
| No reference | 0 |
| Snapshots written | 28 |
| Pass rate | **0.0%** (of 15 comparable) |
| Total time | 1.9s |

## Per-query results

| # | Status | Time (ms) | SQL | Diff summary |
|---:|:--|---:|---|---|
| 1 | ⬇ snapshot | 1044 | SELECT COUNT(*) FROM hits |  |
| 2 | ⬇ snapshot | 80 | SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0 |  |
| 3 | ⬇ snapshot | 35 | SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits |  |
| 4 | ⬇ snapshot | 23 | SELECT AVG(UserID) FROM hits |  |
| 5 | ⬇ snapshot | 17 | SELECT COUNT(DISTINCT UserID) FROM hits |  |
| 6 | ⬇ snapshot | 18 | SELECT COUNT(DISTINCT SearchPhrase) FROM hits |  |
| 7 | ⬇ snapshot | 21 | SELECT MIN(EventDate), MAX(EventDate) FROM hits |  |
| 8 | ⬇ snapshot | 49 | SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*… |  |
| 9 | ⬇ snapshot | 17 | SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10 |  |
| 10 | ⬇ snapshot | 23 | SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM… |  |
| 11 | ⬇ snapshot | 24 | SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP B… |  |
| 12 | ⬇ snapshot | 20 | SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel … |  |
| 13 | ⬇ snapshot | 18 | SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER B… |  |
| 14 | ⬇ snapshot | 17 | SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY Search… |  |
| 15 | ⬇ snapshot | 18 | SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY Sear… |  |
| 16 | ⬇ snapshot | 14 | SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 |  |
| 17 | ⬇ snapshot | 13 | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DES… |  |
| 18 | ⬇ snapshot | 14 | SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10 |  |
| 19 | ✗ fail | 19 | SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserI… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 20 | ⬇ snapshot | 14 | SELECT UserID FROM hits WHERE UserID = 435090932899640449 |  |
| 21 | ⬇ snapshot | 17 | SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%' |  |
| 22 | ✗ fail | 17 | SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 23 | ✗ fail | 14 | SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Ti… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 24 | ⬇ snapshot | 33 | SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 |  |
| 25 | ⬇ snapshot | 15 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10 |  |
| 26 | ⬇ snapshot | 14 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10 |  |
| 27 | ⬇ snapshot | 13 | SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10 |  |
| 28 | ✗ fail | 6 | SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 29 | ✗ fail | 6 | SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) … | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 30 | ✗ fail | 56 | SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidt… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 31 | ⬇ snapshot | 18 | SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHER… |  |
| 32 | ⬇ snapshot | 16 | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE Searc… |  |
| 33 | ⬇ snapshot | 13 | SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY Wa… |  |
| 34 | ⬇ snapshot | 12 | SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10 |  |
| 35 | ✗ fail | 8 | SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 36 | ✗ fail | 7 | SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientI… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 37 | ✗ fail | 58 | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND … | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 38 | ✗ fail | 15 | SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AN… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 39 | ✗ fail | 13 | SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND … | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 40 | ✗ fail | 8 | SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID =… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 41 | ✗ fail | 17 | SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 42 | ✗ fail | 13 | SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 … | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |
| 43 | ✗ fail | 5 | SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 A… | RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:92… |

## Errors

### q19

```sql
SELECT UserID, extract(minute FROM EventTime) AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] No backend supports scalar function [EXTRACT] among [datafusion]
```

### q22

```sql
SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unable to find binding for call MIN($1)
```

### q23

```sql
SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unable to find binding for call MIN($1)
```

### q28

```sql
SELECT CounterID, AVG(length(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Failed to plan query
```

### q29

```sql
SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 400 Bad Request] {   "error": {     "reason": "Invalid SQL query",     "details": "Failed to parse request payload",     "type": "IllegalArgumentException"   },   "status": 400 }
```

### q30

```sql
SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] No backend supports scalar function [PLUS] among [datafusion]
```

### q35

```sql
SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized aggregate function [ANY_VALUE]
```

### q36

```sql
SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] No backend supports scalar function [MINUS] among [datafusion]
```

### q37

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized filter operator [SEARCH]
```

### q38

```sql
SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized filter operator [SEARCH]
```

### q39

```sql
SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized filter operator [SEARCH]
```

### q40

```sql
SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Failed to plan query
```

### q41

```sql
SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized filter operator [SEARCH]
```

### q42

```sql
SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Unrecognized filter operator [SEARCH]
```

### q43

```sql
SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY DATE_TRUNC('minute', EventTime) ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000
```

```
RuntimeException: org.opensearch.client.ResponseException: method [POST], host [http://localhost:9200], URI [/_plugins/_sql?format=jdbc], status line [HTTP/1.1 500 Internal Server Error] Failed to plan query
```

