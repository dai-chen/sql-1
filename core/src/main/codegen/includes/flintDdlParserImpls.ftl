<#--
  Secondary Index Extension grammar: Flint DDL statements.
  Supports skipping index, covering index, materialized view,
  and index management operations.
-->

<#-- CREATE SKIPPING INDEX [IF NOT EXISTS] ON table (col type, ...) -->
SqlCreate SqlCreateSkippingIndex(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    SqlIdentifier tableName;
    List<SqlNode> columns = new ArrayList<SqlNode>();
}
{
    <SKIPPING> <INDEX>
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    <ON> tableName = CompoundIdentifier()
    <LPAREN>
    {
        SqlIdentifier col = SimpleIdentifier();
        SqlIdentifier colType = SimpleIdentifier();
        columns.add(new SqlNodeList(java.util.Arrays.asList(col, colType), getPos()));
    }
    (
        <COMMA>
        {
            SqlIdentifier col2 = SimpleIdentifier();
            SqlIdentifier colType2 = SimpleIdentifier();
            columns.add(new SqlNodeList(java.util.Arrays.asList(col2, colType2), getPos()));
        }
    )*
    <RPAREN>
    {
        return new SqlCreateSkippingIndex(s.end(this), ifNotExists, tableName,
            new SqlNodeList(columns, s.end(this)));
    }
}

<#-- CREATE INDEX [IF NOT EXISTS] name ON table (col, ...) -->
SqlCreate SqlCreateCoveringIndex(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    SqlIdentifier indexName;
    SqlIdentifier tableName;
    SqlNodeList columns;
}
{
    <INDEX>
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    indexName = SimpleIdentifier()
    <ON> tableName = CompoundIdentifier()
    columns = ParenthesizedSimpleIdentifierList()
    {
        return new SqlCreateCoveringIndex(s.end(this), ifNotExists, indexName, tableName, columns);
    }
}

<#-- CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS query -->
SqlCreate SqlCreateMaterializedView(Span s, boolean replace) :
{
    boolean ifNotExists = false;
    SqlIdentifier viewName;
    SqlNode query;
}
{
    <MATERIALIZED> <VIEW>
    [ <IF> <NOT> <EXISTS> { ifNotExists = true; } ]
    viewName = CompoundIdentifier()
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        return new SqlCreateMaterializedView(s.end(this), ifNotExists, viewName, query);
    }
}

<#-- DROP SKIPPING INDEX ON table | DROP INDEX name ON table | DROP MATERIALIZED VIEW name -->
SqlDrop SqlDropFlintIndex(Span s, boolean replace) :
{
    boolean ifExists = false;
    String indexType;
    SqlIdentifier indexName = null;
    SqlIdentifier tableName = null;
}
{
    (
        <SKIPPING> <INDEX>
        { indexType = "SKIPPING"; }
        [ <IF> <EXISTS> { ifExists = true; } ]
        <ON> tableName = CompoundIdentifier()
    |
        <INDEX>
        { indexType = "COVERING"; }
        [ <IF> <EXISTS> { ifExists = true; } ]
        indexName = SimpleIdentifier()
        <ON> tableName = CompoundIdentifier()
    |
        <MATERIALIZED> <VIEW>
        { indexType = "MATERIALIZED_VIEW"; }
        [ <IF> <EXISTS> { ifExists = true; } ]
        indexName = CompoundIdentifier()
    )
    {
        return new SqlDropFlintIndex(s.end(this), ifExists, indexType, indexName, tableName);
    }
}

<#-- REFRESH SKIPPING INDEX ON table | REFRESH INDEX name ON table | REFRESH MATERIALIZED VIEW name -->
SqlNode SqlRefreshFlintIndex() :
{
    SqlParserPos pos;
    String indexType;
    SqlIdentifier indexName = null;
    SqlIdentifier tableName = null;
}
{
    <REFRESH> { pos = getPos(); }
    (
        <SKIPPING> <INDEX>
        { indexType = "SKIPPING"; }
        <ON> tableName = CompoundIdentifier()
    |
        <INDEX>
        { indexType = "COVERING"; }
        indexName = SimpleIdentifier()
        <ON> tableName = CompoundIdentifier()
    |
        <MATERIALIZED> <VIEW>
        { indexType = "MATERIALIZED_VIEW"; }
        indexName = CompoundIdentifier()
    )
    {
        return new SqlRefreshFlintIndex(pos, indexType, indexName, tableName);
    }
}

<#-- SHOW FLINT INDEX[ES] IN catalog.database -->
SqlNode SqlShowFlintIndexes() :
{
    SqlParserPos pos;
    SqlIdentifier catalogDb;
}
{
    <SHOW> { pos = getPos(); }
    <FLINT>
    ( <INDEX> | <INDEXES> )
    <IN>
    catalogDb = CompoundIdentifier()
    {
        return new SqlShowFlintIndexes(pos, catalogDb);
    }
}

<#-- RECOVER INDEX JOB jobId -->
SqlNode SqlRecoverIndexJob() :
{
    SqlParserPos pos;
    SqlIdentifier jobId;
}
{
    <RECOVER> { pos = getPos(); }
    <INDEX> <JOB>
    jobId = SimpleIdentifier()
    {
        return new SqlRecoverIndexJob(pos, jobId);
    }
}
