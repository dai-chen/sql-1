<#--
  Search Extension grammar: named parameters and array literals.
  These extend Calcite's function call syntax for OpenSearch relevance functions.
-->

<#-- No custom statement methods needed for Search Extension.
     Named params and array literals are handled at the expression level
     by overriding the function argument parsing in the operator table
     and validator, not in the grammar.

     For this PoC, the Babel parser already handles:
     - Backtick quoting (via Lex.MYSQL)
     - MATCH de-reservation (via SqlBabelParserImpl)
     - Standard function calls with positional args

     Named params (key=value) and array literals ([f1, f2]) would require
     modifying the Expression() production in Parser.jj, which is complex.
     For the PoC, we demonstrate these as TODO items and focus on proving
     the grammar extension mechanism works via Flint DDL.
-->
