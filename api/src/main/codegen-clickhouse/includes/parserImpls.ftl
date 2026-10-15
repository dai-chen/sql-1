<#-- ClickHouse dialect (PoC) parser productions. -->

<#-- LEFT SEMI JOIN / LEFT ANTI JOIN (registered via parser.joinTypes). -->
JoinType LeftSemiJoin() :
{
}
{
    <LEFT> <SEMI> <JOIN> { return JoinType.LEFT_SEMI_JOIN; }
}

JoinType LeftAntiJoin() :
{
}
{
    <LEFT> <ANTI> <JOIN> { return JoinType.LEFT_ANTI_JOIN; }
}

<#-- ClickHouse array literal [e1, e2, ...], equivalent to standard ARRAY[e1, e2, ...]. -->
SqlNode ClickHouseArrayLiteral() :
{
    final SqlNodeList args;
    final Span s;
}
{
    <LBRACKET> { s = span(); }
    (
        args = ExpressionCommaList(s, ExprContext.ACCEPT_SUB_QUERY)
    |
        { args = SqlNodeList.EMPTY; }
    )
    <RBRACKET>
    {
        return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
            s.end(this), args.getList());
    }
}
