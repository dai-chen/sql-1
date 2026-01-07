# Federated Search Design for OpenSearch PPL

## Overview

This document outlines the design for implementing federated search capabilities in OpenSearch PPL, enabling unified queries across hot OpenSearch indices and cold S3 data storage, similar to Splunk's federated search functionality.

## Problem Statement

Organizations need to query data across multiple storage tiers:
- **Hot data**: Recent data in OpenSearch indices (fast, expensive)
- **Cold data**: Historical data in S3 (slower, cost-effective)

Current limitations:
- Users must query hot and cold data separately
- No unified query interface across storage tiers
- Manual data stitching required for time-series analysis
- Complex application logic to handle multi-tier queries

## Goals

1. **Transparent Query Federation**: Single PPL query spans hot and cold data
2. **Automatic Tier Selection**: Query planner determines optimal data sources
3. **Time-Based Partitioning**: Leverage time ranges to route queries efficiently
4. **Performance Optimization**: Minimize data movement and query latency
5. **Backward Compatibility**: Existing queries continue to work unchanged

## Architecture Overview

### High-Level Components

```
┌─────────────────────────────────────────────────────────────┐
│                     PPL Query Layer                          │
│  (Federated Search Syntax + Query Planning)                 │
└────────────────┬────────────────────────────────────────────┘
                 │
    ┌────────────┴────────────┐
    │                         │
┌───▼────────────┐    ┌──────▼──────────┐
│  Hot Storage   │    │  Cold Storage   │
│  (OpenSearch)  │    │  (S3 + Spark)   │
└────────────────┘    └─────────────────┘
```

### Key Components

1. **Federated Table Registry**: Maps logical tables to physical storage locations
2. **Query Planner Extension**: Routes queries to appropriate storage tiers
3. **S3 Data Source Connector**: Integrates S3 data via Spark/Athena
4. **Result Merger**: Combines results from multiple sources
5. **Metadata Service**: Tracks data location and time ranges

## Detailed Design

### 1. Federated Table Definition

#### PPL Syntax Extension

```sql
-- Define federated table with hot and cold tiers
CREATE FEDERATED TABLE logs (
  timestamp TIMESTAMP,
  level STRING,
  message STRING,
  host STRING
) WITH (
  hot_source = 'opensearch.logs_hot',
  cold_source = 's3glue.logs_archive',
  partition_column = 'timestamp',
  hot_retention_days = 7,
  transition_policy = 'time_based'
);

-- Query federated table transparently
source=logs 
| where timestamp > now() - 30d 
| stats count() by level;
```

#### Alternative: Implicit Federation via Naming Convention

```sql
-- Automatic federation based on index pattern
source=logs_*  -- Matches logs_hot (OpenSearch) and logs_archive (S3)
| where timestamp between '2024-01-01' and '2024-12-31'
| stats avg(response_time) by service;
```

### 2. Data Source Model Extension

#### New Classes

```java
// datasources/src/main/java/org/opensearch/sql/datasources/model/

/**
 * Represents a federated data source spanning multiple storage tiers
 */
@Data
@Builder
public class FederatedDataSource {
  private String name;
  private List<TierDefinition> tiers;
  private PartitionStrategy partitionStrategy;
  private FederationPolicy policy;
}

/**
 * Defines a single storage tier in federated setup
 */
@Data
@Builder
public class TierDefinition {
  private String tierName;  // "hot", "warm", "cold"
  private DataSource dataSource;
  private TimeRange dataRange;  // Optional: time range for this tier
  private int priority;  // Query routing priority
  private Map<String, String> properties;
}

/**
 * Strategy for partitioning data across tiers
 */
public enum PartitionStrategy {
  TIME_BASED,      // Partition by time column
  SIZE_BASED,      // Partition by data size
  CUSTOM           // User-defined partitioning logic
}

/**
 * Policy for federated query execution
 */
@Data
@Builder
public class FederationPolicy {
  private boolean allowCrossSource;  // Allow queries spanning sources
  private boolean preferHotData;     // Prefer hot tier when possible
  private Duration maxQueryTime;     // Max query execution time
  private CostOptimization costStrategy;
}
```

### 3. Query Planning Extensions

#### Federated Query Planner

```java
// core/src/main/java/org/opensearch/sql/planner/federated/

/**
 * Plans federated queries across multiple data sources
 */
public class FederatedQueryPlanner {
  
  private final DataSourceService dataSourceService;
  private final MetadataService metadataService;
  
  /**
   * Analyzes query and determines which data sources to query
   */
  public FederatedQueryPlan planQuery(
      UnresolvedPlan ast, 
      QueryContext context) {
    
    // 1. Extract table references and predicates
    TableExtractor extractor = new TableExtractor();
    List<TableReference> tables = extractor.extract(ast);
    
    // 2. Resolve federated tables
    List<FederatedDataSource> fedSources = 
        resolveFederatedSources(tables);
    
    // 3. Analyze predicates for tier selection
    PredicateAnalyzer analyzer = new PredicateAnalyzer();
    TierSelectionHints hints = analyzer.analyze(ast, fedSources);
    
    // 4. Generate sub-plans for each tier
    List<SubQueryPlan> subPlans = 
        generateSubPlans(ast, fedSources, hints);
    
    // 5. Create merge plan
    return FederatedQueryPlan.builder()
        .subPlans(subPlans)
        .mergeStrategy(determineMergeStrategy(ast))
        .build();
  }
  
  /**
   * Determines which tiers to query based on predicates
   */
  private TierSelectionHints analyzeTierSelection(
      UnresolvedPlan ast,
      FederatedDataSource fedSource) {
    
    // Extract time range from WHERE clause
    Optional<TimeRange> queryRange = extractTimeRange(ast);
    
    TierSelectionHints.Builder hints = TierSelectionHints.builder();
    
    for (TierDefinition tier : fedSource.getTiers()) {
      if (shouldQueryTier(tier, queryRange)) {
        hints.addTier(tier);
      }
    }
    
    return hints.build();
  }
}

/**
 * Represents a federated query execution plan
 */
@Data
@Builder
public class FederatedQueryPlan {
  private List<SubQueryPlan> subPlans;
  private MergeStrategy mergeStrategy;
  private ExecutionMode mode;  // PARALLEL, SEQUENTIAL
  
  public enum MergeStrategy {
    UNION_ALL,      // Simple concatenation
    UNION_DISTINCT, // Deduplicate results
    TIME_ORDERED,   // Merge by timestamp
    CUSTOM          // User-defined merge logic
  }
}

/**
 * Sub-query plan for a single data source
 */
@Data
@Builder
public class SubQueryPlan {
  private DataSource dataSource;
  private UnresolvedPlan queryAst;
  private List<String> projections;
  private Optional<Predicate> pushdownFilter;
}
```

### 4. PPL Grammar Extensions

#### ANTLR Grammar Changes

```antlr
// ppl/src/main/antlr/OpenSearchPPLParser.g4

// Add federated search command
federatedSearchCommand
    : SEARCH FEDERATED? tableSourceClause whereClause?
    ;

tableSourceClause
    : tableName (COMMA tableName)*
    | federatedTableName
    ;

federatedTableName
    : IDENTIFIER (DOT IDENTIFIER)?  // e.g., logs or federated.logs
    ;

// Add CREATE FEDERATED TABLE statement
createFederatedTableStatement
    : CREATE FEDERATED TABLE tableName
      LPAREN columnDefinitions RPAREN
      WITH LPAREN federatedOptions RPAREN
    ;

federatedOptions
    : federatedOption (COMMA federatedOption)*
    ;

federatedOption
    : HOT_SOURCE EQUALS stringLiteral
    | COLD_SOURCE EQUALS stringLiteral
    | PARTITION_COLUMN EQUALS IDENTIFIER
    | HOT_RETENTION_DAYS EQUALS integerLiteral
    | TRANSITION_POLICY EQUALS stringLiteral
    ;
```

### 5. S3 Data Source Integration

#### S3 Connector Implementation

```java
// datasources/src/main/java/org/opensearch/sql/datasources/s3/

/**
 * S3-based data source for cold storage queries
 */
public class S3DataSource implements DataSource {
  
  private final S3Client s3Client;
  private final SparkSession sparkSession;  // For query execution
  private final String bucketName;
  private final String prefix;
  private final DataFormat format;  // PARQUET, ORC, JSON
  
  @Override
  public PhysicalPlan createScanPlan(
      LogicalPlan logicalPlan,
      PushDownContext context) {
    
    // Convert logical plan to Spark SQL
    String sparkSql = convertToSparkSQL(logicalPlan);
    
    // Create S3 scan with predicate pushdown
    return S3ScanPlan.builder()
        .s3Location(buildS3Location(context))
        .sparkQuery(sparkSql)
        .schema(inferSchema(logicalPlan))
        .build();
  }
  
  /**
   * Executes query against S3 data using Spark
   */
  public QueryResult executeQuery(S3ScanPlan plan) {
    // Use existing async-query infrastructure
    // Leverage S3GlueDataSourceSparkParameterComposer
    
    Dataset<Row> df = sparkSession.read()
        .format(format.name().toLowerCase())
        .load(plan.getS3Location());
    
    // Apply filters and projections
    Dataset<Row> result = df.sqlContext().sql(plan.getSparkQuery());
    
    return convertToQueryResult(result);
  }
}

/**
 * Metadata service for tracking data locations
 */
public class FederatedMetadataService {
  
  private final OpenSearchClient client;
  private static final String METADATA_INDEX = ".opensearch-federated-metadata";
  
  /**
   * Registers data location metadata
   */
  public void registerDataLocation(DataLocationMetadata metadata) {
    // Store in OpenSearch index
    client.index(req -> req
        .index(METADATA_INDEX)
        .document(metadata));
  }
  
  /**
   * Queries metadata to determine data locations
   */
  public List<DataLocation> findDataLocations(
      String tableName,
      Optional<TimeRange> timeRange) {
    
    // Query metadata index
    SearchRequest request = buildMetadataQuery(tableName, timeRange);
    SearchResponse<DataLocationMetadata> response = 
        client.search(request, DataLocationMetadata.class);
    
    return response.hits().hits().stream()
        .map(hit -> hit.source().toDataLocation())
        .collect(Collectors.toList());
  }
}
```

### 6. Result Merging

#### Federated Result Merger

```java
// core/src/main/java/org/opensearch/sql/executor/federated/

/**
 * Merges results from multiple data sources
 */
public class FederatedResultMerger {
  
  /**
   * Merges results based on merge strategy
   */
  public QueryResult merge(
      List<QueryResult> results,
      MergeStrategy strategy,
      Schema schema) {
    
    switch (strategy) {
      case UNION_ALL:
        return unionAll(results, schema);
      
      case TIME_ORDERED:
        return timeOrderedMerge(results, schema);
      
      case UNION_DISTINCT:
        return unionDistinct(results, schema);
      
      default:
        throw new UnsupportedOperationException(
            "Merge strategy not supported: " + strategy);
    }
  }
  
  /**
   * Time-ordered merge for time-series data
   */
  private QueryResult timeOrderedMerge(
      List<QueryResult> results,
      Schema schema) {
    
    // Use priority queue for efficient merging
    PriorityQueue<ResultIterator> queue = new PriorityQueue<>(
        Comparator.comparing(iter -> 
            iter.current().getTimestamp()));
    
    results.forEach(result -> 
        queue.offer(new ResultIterator(result)));
    
    List<Row> mergedRows = new ArrayList<>();
    while (!queue.isEmpty()) {
      ResultIterator iter = queue.poll();
      mergedRows.add(iter.current());
      
      if (iter.hasNext()) {
        iter.next();
        queue.offer(iter);
      }
    }
    
    return new QueryResult(schema, mergedRows);
  }
}
```

### 7. Configuration and Management

#### Federated Table Configuration

```yaml
# config/federated-tables.yml

federated_tables:
  - name: application_logs
    hot_source:
      type: opensearch
      index: logs-*
      retention_days: 7
    cold_source:
      type: s3glue
      database: logs_archive
      table: application_logs
      location: s3://my-bucket/logs/
      format: parquet
    partition:
      column: timestamp
      strategy: time_based
    policy:
      allow_cross_source: true
      prefer_hot_data: true
      max_query_time: 5m

  - name: metrics
    hot_source:
      type: opensearch
      index: metrics-*
      retention_days: 30
    cold_source:
      type: s3glue
      database: metrics_archive
      table: historical_metrics
      location: s3://my-bucket/metrics/
      format: orc
    partition:
      column: collected_at
      strategy: time_based
```

#### REST API for Management

```java
// datasources/src/main/java/org/opensearch/sql/datasources/rest/

/**
 * REST API for federated table management
 */
@RestController
public class FederatedTableManagementAction extends BaseRestHandler {
  
  @Override
  public List<Route> routes() {
    return List.of(
        // Create federated table
        new Route(POST, "/_plugins/_query/federated/tables"),
        
        // List federated tables
        new Route(GET, "/_plugins/_query/federated/tables"),
        
        // Get federated table details
        new Route(GET, "/_plugins/_query/federated/tables/{name}"),
        
        // Update federated table
        new Route(PUT, "/_plugins/_query/federated/tables/{name}"),
        
        // Delete federated table
        new Route(DELETE, "/_plugins/_query/federated/tables/{name}")
    );
  }
}
```

## Implementation Phases

### Phase 1: Foundation (Weeks 1-3)
- [ ] Design federated data model
- [ ] Implement FederatedDataSource and TierDefinition classes
- [ ] Create metadata service for tracking data locations
- [ ] Add configuration support for federated tables

### Phase 2: Query Planning (Weeks 4-6)
- [ ] Implement FederatedQueryPlanner
- [ ] Add tier selection logic based on predicates
- [ ] Create sub-query plan generation
- [ ] Implement predicate pushdown for S3 sources

### Phase 3: S3 Integration (Weeks 7-9)
- [ ] Implement S3DataSource connector
- [ ] Integrate with existing async-query Spark infrastructure
- [ ] Add support for Parquet/ORC formats
- [ ] Implement partition pruning for S3 data

### Phase 4: Result Merging (Weeks 10-11)
- [ ] Implement FederatedResultMerger
- [ ] Add time-ordered merge strategy
- [ ] Implement union and distinct merge strategies
- [ ] Add result streaming for large datasets

### Phase 5: PPL Syntax (Weeks 12-13)
- [ ] Extend PPL grammar for federated search
- [ ] Add CREATE FEDERATED TABLE support
- [ ] Implement AST builders for federated commands
- [ ] Add query validation

### Phase 6: Testing & Optimization (Weeks 14-16)
- [ ] Unit tests for all components
- [ ] Integration tests with OpenSearch + S3
- [ ] Performance benchmarking
- [ ] Query optimization tuning
- [ ] Documentation

## Query Examples

### Example 1: Time-Range Query Spanning Hot and Cold

```sql
-- Query last 60 days (7 days hot + 53 days cold)
source=application_logs
| where timestamp > now() - 60d
| where level = 'ERROR'
| stats count() by date_trunc('day', timestamp)
| sort timestamp;

-- Execution plan:
-- 1. Query OpenSearch for last 7 days
-- 2. Query S3 for days 8-60
-- 3. Merge results by timestamp
```

### Example 2: Aggregation Across Tiers

```sql
-- Monthly aggregation spanning multiple tiers
source=metrics
| where collected_at between '2024-01-01' and '2024-12-31'
| stats avg(cpu_usage), max(memory_usage) by host, date_trunc('month', collected_at)
| sort collected_at;

-- Execution plan:
-- 1. Partial aggregation on hot data (recent months)
-- 2. Partial aggregation on cold data (older months)
-- 3. Combine partial aggregations
```

### Example 3: Explicit Tier Selection

```sql
-- Query only cold storage
source=application_logs@cold
| where timestamp between '2023-01-01' and '2023-12-31'
| stats count() by error_code;

-- Query only hot storage
source=application_logs@hot
| where timestamp > now() - 1d
| head 100;
```

## Performance Considerations

### Query Optimization Strategies

1. **Predicate Pushdown**: Push filters to S3 (Parquet/ORC predicates)
2. **Partition Pruning**: Skip S3 partitions outside time range
3. **Parallel Execution**: Query hot and cold tiers concurrently
4. **Result Streaming**: Stream results to avoid memory issues
5. **Caching**: Cache S3 metadata and frequently accessed data

### Cost Optimization

1. **Smart Tier Selection**: Query only necessary tiers
2. **Columnar Formats**: Use Parquet/ORC for efficient S3 scans
3. **Compression**: Enable compression for S3 data
4. **Query Result Caching**: Cache expensive S3 query results

## Security Considerations

1. **Access Control**: Unified permissions across hot and cold data
2. **Data Encryption**: Encrypt S3 data at rest and in transit
3. **Audit Logging**: Track federated queries for compliance
4. **IAM Integration**: Use AWS IAM for S3 access control

## Monitoring and Observability

### Metrics to Track

- Query latency by tier (hot vs cold)
- Data scanned per tier
- S3 API call costs
- Cache hit rates
- Query failure rates by tier

### Logging

- Federated query execution plans
- Tier selection decisions
- S3 query performance
- Merge operation timing

## Future Enhancements

1. **Warm Tier Support**: Add intermediate warm tier (e.g., S3 Glacier Instant Retrieval)
2. **Automatic Tiering**: Auto-move data based on access patterns
3. **Cross-Region Federation**: Query data across AWS regions
4. **Multi-Cloud Support**: Extend to Azure Blob, GCS
5. **Smart Caching**: Predictive caching of cold data
6. **Query Acceleration**: Materialized views for common queries

## References

- Splunk Federated Search: https://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Federated
- OpenSearch Data Sources: `datasources/` module
- Async Query Infrastructure: `async-query/` module
- S3Glue Integration: `S3GlueDataSourceSparkParameterComposer`
