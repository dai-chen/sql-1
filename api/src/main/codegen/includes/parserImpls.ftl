<#--
  Copyright OpenSearch Contributors
  SPDX-License-Identifier: Apache-2.0
-->

/**
 * Optionally parses EXCEPT / REPLACE clauses after a star (*) or qualified star (t.*).
 * If neither EXCEPT nor REPLACE follows, returns the star node unchanged.
 */
SqlNode StarExceptReplaceOpt(SqlNode star) :
{
    SqlNodeList exceptList = null;
    SqlNodeList replaceList = null;
}
{
    (
        LOOKAHEAD(2, <EXCEPT> <LPAREN>)
        <EXCEPT>
        <LPAREN> { exceptList = new SqlNodeList(getPos()); }
        { SqlIdentifier col; }
        col = SimpleIdentifier() { exceptList.add(col); }
        ( <COMMA> col = SimpleIdentifier() { exceptList.add(col); } )*
        <RPAREN>
    )?
    (
        LOOKAHEAD(2, <REPLACE> <LPAREN>)
        <REPLACE>
        <LPAREN> { replaceList = new SqlNodeList(getPos()); }
        { SqlNode expr; SqlIdentifier alias; }
        expr = Expression(ExprContext.ACCEPT_NON_QUERY) <AS> alias = SimpleIdentifier()
        {
            replaceList.add(
                SqlStdOperatorTable.AS.createCall(span().end(expr), expr, alias));
        }
        (
            <COMMA>
            expr = Expression(ExprContext.ACCEPT_NON_QUERY) <AS> alias = SimpleIdentifier()
            {
                replaceList.add(
                    SqlStdOperatorTable.AS.createCall(span().end(expr), expr, alias));
            }
        )*
        <RPAREN>
    )?
    {
        if (exceptList == null && replaceList == null) {
            return star;
        }
        return new SqlStarExceptReplace(
            star.getParserPosition(), star, exceptList, replaceList);
    }
}
