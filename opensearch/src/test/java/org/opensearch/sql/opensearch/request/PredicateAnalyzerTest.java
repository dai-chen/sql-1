/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Sarg;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.ExpressionNotAnalyzableException;
import org.opensearch.sql.opensearch.request.PredicateAnalyzer.QueryExpression;

public class PredicateAnalyzerTest {
  final OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
  final RexBuilder builder = new RexBuilder(typeFactory);
  final RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), builder);
  final List<String> schema = List.of("a", "b", "c", "d", "e", "f", "g");
  final Map<String, ExprType> fieldTypes =
      Map.ofEntries(
          Map.entry("a", OpenSearchDataType.of(MappingType.Integer)),
          Map.entry(
              "b",
              OpenSearchDataType.of(
                  MappingType.Text,
                  Map.of("fields", Map.of("keyword", Map.of("type", "keyword"))))),
          Map.entry(
              "c",
              OpenSearchDataType.of(MappingType.Text)), // Text without keyword cannot be push down
          Map.entry("d", OpenSearchDataType.of(MappingType.Date)),
          Map.entry("e", OpenSearchDataType.of(MappingType.Boolean)),
          Map.entry("f", OpenSearchDataType.of(MappingType.Ip)),
          Map.entry("g", OpenSearchDataType.of(MappingType.Object)));
  final RexInputRef field1 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
  final RexInputRef field2 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 1);
  final RexInputRef field4 = builder.makeInputRef(typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), 3);
  final RexInputRef field5 =
      builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 4);
  final RexLiteral numericLiteral = builder.makeExactLiteral(new BigDecimal(12));
  final RexLiteral stringLiteral = builder.makeLiteral("Hi");
  final RexNode dateTimeLiteral =
      builder.makeLiteral(
          "1987-02-03 04:34:56", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP), true);
  final RexNode aliasedField2 =
      builder.makeCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, builder.makeLiteral("field"), field2);
  final RexNode aliasedStringLiteral =
      builder.makeCall(
          SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, builder.makeLiteral("query"), stringLiteral);

  @Test
  void equals_generatesTermQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "a" : {
              "value" : 12,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notEquals_generatesBoolQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "must_not" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gt_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : 12,
              "to" : null,
              "include_lower" : false,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gte_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : 12,
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void lt_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.LESS_THAN, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : null,
              "to" : 12,
              "include_lower" : true,
              "include_upper" : false,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void lte_generatesRangeQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, field1, numericLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "a" : {
              "from" : null,
              "to" : 12,
              "include_lower" : true,
              "include_upper" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void exists_generatesExistsQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, field1);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(ExistsQueryBuilder.class, result);
    assertEquals(
        """
        {
          "exists" : {
            "field" : "a",
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notExists_generatesMustNotExistsQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_NULL, field1);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void search_generatesTermsQuery() throws ExpressionNotAnalyzableException {
    final RexLiteral numericLiteral1 = builder.makeExactLiteral(new BigDecimal(13));
    final RexLiteral numericLiteral2 = builder.makeExactLiteral(new BigDecimal(14));
    RexNode call =
        builder.makeIn(field1, ImmutableList.of(numericLiteral, numericLiteral1, numericLiteral2));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermsQueryBuilder.class, result);
    assertEquals(
        """
        {
          "terms" : {
            "a" : [
              12.0,
              13.0,
              14.0
            ],
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void contains_generatesMatchQuery() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.CONTAINS, field2, stringLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(MatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "lenient" : false,
              "zero_terms_query" : "NONE",
              "auto_generate_synonyms_phrase_query" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchRelevanceQueryFunction_generatesMatchQuery() throws ExpressionNotAnalyzableException {
    List<RexNode> arguments = Arrays.asList(aliasedField2, aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "lenient" : false,
              "zero_terms_query" : "NONE",
              "auto_generate_synonyms_phrase_query" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchPhraseRelevanceQueryFunction_generatesMatchPhraseQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("slop"),
                builder.makeLiteral("2")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_phrase", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchPhraseQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_phrase" : {
            "b" : {
              "query" : "Hi",
              "slop" : 2,
              "zero_terms_query" : "NONE",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchBoolPrefixRelevanceQueryFunction_generatesMatchBoolPrefixQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("minimum_should_match"),
                builder.makeLiteral("1")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_bool_prefix", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchBoolPrefixQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_bool_prefix" : {
            "b" : {
              "query" : "Hi",
              "operator" : "OR",
              "minimum_should_match" : "1",
              "prefix_length" : 0,
              "max_expansions" : 50,
              "fuzzy_transpositions" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void matchPhrasePrefixRelevanceQueryFunction_generatesMatchPhrasePrefixQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            aliasedField2,
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("analyzer"),
                builder.makeLiteral("standard")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "match_phrase_prefix", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MatchPhrasePrefixQueryBuilder.class, result);
    assertEquals(
        """
        {
          "match_phrase_prefix" : {
            "b" : {
              "query" : "Hi",
              "analyzer" : "standard",
              "slop" : 0,
              "max_expansions" : 50,
              "zero_terms_query" : "NONE",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void queryStringRelevanceQueryFunction_generatesQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true),
                    builder.makeLiteral("c"),
                    builder.makeLiteral(
                        2.5, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fuzziness"),
                builder.makeLiteral("1")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(QueryStringQueryBuilder.class, result);
    assertEquals(
        """
        {
          "query_string" : {
            "query" : "Hi",
            "fields" : [
              "b^1.0",
              "c^2.5"
            ],
            "type" : "best_fields",
            "default_operator" : "or",
            "max_determinized_states" : 10000,
            "enable_position_increments" : true,
            "fuzziness" : "1",
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "phrase_slop" : 0,
            "escape" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void simpleQueryStringRelevanceQueryFunction_generatesSimpleQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "simple_query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(SimpleQueryStringBuilder.class, result);
    assertEquals(
        """
        {
          "simple_query_string" : {
            "query" : "Hi",
            "fields" : [
              "b*^1.0"
            ],
            "flags" : -1,
            "default_operator" : "or",
            "analyze_wildcard" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void multiMatchRelevanceQueryFunction_generatesMultiMatchQuery()
      throws ExpressionNotAnalyzableException {
    List<RexNode> arguments =
        Arrays.asList(
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("fields"),
                builder.makeCall(
                    SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                    builder.makeLiteral("b*"),
                    builder.makeLiteral(
                        1.0, builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), true))),
            aliasedStringLiteral,
            builder.makeCall(
                SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
                builder.makeLiteral("max_expansions"),
                builder.makeLiteral("25")));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "multi_match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);
    assertInstanceOf(MultiMatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "multi_match" : {
            "query" : "Hi",
            "fields" : [
              "b*^1.0"
            ],
            "type" : "best_fields",
            "operator" : "OR",
            "slop" : 0,
            "prefix_length" : 0,
            "max_expansions" : 25,
            "zero_terms_query" : "NONE",
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void likeFunction_keywordField_isNotPushable() {
    List<RexNode> arguments =
        Arrays.asList(field2, builder.makeLiteral("%Hi%"), builder.makeLiteral(true));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "like", arguments.toArray(new RexNode[0]));
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyze(call, schema, fieldTypes));
  }

  @Test
  void ilikeFunction_keywordField_isNotPushable() {
    List<RexNode> arguments = Arrays.asList(field2, builder.makeLiteral("%Hi%"));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "ilike", arguments.toArray(new RexNode[0]));
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyze(call, schema, fieldTypes));
  }

  @Test
  void likeFunction_textField_isNotPushable() {
    RexInputRef field3 = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    List<RexNode> arguments =
        Arrays.asList(field3, builder.makeLiteral("%Hi%"), builder.makeLiteral(true));
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "like", arguments.toArray(new RexNode[0]));

    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .add("c", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    // LIKE on text field without .keyword is not index-accelerable; must surface as unanalyzable
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyzeExpression(call, schema, fieldTypes, rowType, cluster));
  }

  @Test
  void andOrNot_generatesCompoundQuery() throws ExpressionNotAnalyzableException {
    RexNode call1 = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode call2 =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS, field1, builder.makeExactLiteral(new BigDecimal(13)));
    RexNode call3 = builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral);
    RexNode orCall = builder.makeCall(SqlStdOperatorTable.OR, call1, call2);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, orCall, call3);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, andCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "bool" : {
                  "must" : [
                    {
                      "bool" : {
                        "should" : [
                          {
                            "term" : {
                              "a" : {
                                "value" : 12,
                                "boost" : 1.0
                              }
                            }
                          },
                          {
                            "term" : {
                              "a" : {
                                "value" : 13,
                                "boost" : 1.0
                              }
                            }
                          }
                        ],
                        "adjust_pure_negative" : true,
                        "boost" : 1.0
                      }
                    },
                    {
                      "term" : {
                        "b.keyword" : {
                          "value" : "Hi",
                          "boost" : 1.0
                        }
                      }
                    }
                  ],
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_generatesTermQuery_TextWithKeyword() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "b.keyword" : {
              "value" : "Hi",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_textWithoutKeyword_isNotPushable() {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .add("c", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    final RexInputRef field3 =
        builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field3, stringLiteral);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    // Equals on text field without .keyword sub-field is not index-accelerable
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyze(call, schema, fieldTypes, rowType, cluster));
  }

  @Test
  void equals_struct_isNotPushable() {
    final RelDataType mapType =
        typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR),
            typeFactory.createSqlType(SqlTypeName.VARCHAR));
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("d", mapType)
            .build();
    final RexInputRef field4 = builder.makeInputRef(mapType, 0);
    final Map<String, ExprType> newFieldTypes =
        Map.of("d", OpenSearchDataType.of(ExprCoreType.STRUCT));
    final List<String> newSchema = List.of("d");
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_EMPTY, field4);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    // IS_EMPTY on struct is not index-accelerable
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyze(call, newSchema, newFieldTypes, rowType, cluster));
  }

  @Test
  void isTrue_predicate() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(
            SqlStdOperatorTable.IS_TRUE,
            builder.makeCall(SqlStdOperatorTable.EQUALS, field2, stringLiteral));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "b.keyword" : {
              "value" : "Hi",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isEmpty_isNotPushable_becauseOrContainsNonPushableArm() {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    // PPL isempty(x) lowers to OR(IS_NULL(x), CHAR_LENGTH(x) = 0) (see PPLFuncImpTable).
    // The IS_NULL arm is pushable (exists query), but the CHAR_LENGTH arm is NOT
    // index-accelerable. An OR with at least one non-pushable disjunct is entirely
    // non-pushable — it surfaces as ExpressionNotAnalyzableException and becomes a
    // residual Filter evaluated by Calcite.
    RexNode call = PPLFuncImpTable.INSTANCE.resolve(builder, BuiltinFunctionName.IS_EMPTY, field2);
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyzeExpression(call, schema, fieldTypes, rowType, cluster));
  }

  @Test
  void verify_partial_pushdown() throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT))
            .add("b", builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR))
            .build();
    RexNode call1 = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode call2 = builder.makeCall(SqlStdOperatorTable.IS_EMPTY, field2);
    // Partial push down part of and
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, List.of(call1, call2));
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    PredicateAnalyzer.Visitor visitor =
        new PredicateAnalyzer.Visitor(schema, fieldTypes, rowType, cluster);
    PredicateAnalyzer.Visitor visitSpy = spy(visitor);
    Mockito.doThrow(new PredicateAnalyzer.PredicateAnalyzerException(""))
        .when(visitSpy)
        .tryAnalyzeOperand(call2);
    QueryExpression result =
        PredicateAnalyzer.analyzeExpression(
            andCall, schema, fieldTypes, rowType, cluster, visitSpy);

    QueryBuilder resultBuilder = result.builder();
    assertInstanceOf(BoolQueryBuilder.class, resultBuilder);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        resultBuilder.toString());

    List<RexNode> unAnalyzableNodes = result.getUnAnalyzableNodes();
    assertEquals(1, unAnalyzableNodes.size());
    assertEquals(call2, unAnalyzableNodes.getFirst());

    // An OR with a non-pushable disjunct is entirely non-pushable (no partial pushdown for OR).
    RexNode orCall = builder.makeCall(SqlStdOperatorTable.OR, List.of(call1, call2));
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () ->
            PredicateAnalyzer.analyzeExpression(
                orCall, schema, fieldTypes, rowType, cluster, visitSpy));

    Mockito.doThrow(new PredicateAnalyzer.PredicateAnalyzerException(""))
        .when(visitSpy)
        .tryAnalyzeOperand(orCall);
    RexNode thenAndCall = builder.makeCall(SqlStdOperatorTable.AND, List.of(orCall, call1));
    result =
        PredicateAnalyzer.analyzeExpression(
            thenAndCall, schema, fieldTypes, rowType, cluster, visitSpy);
    resultBuilder = result.builder();
    assertInstanceOf(BoolQueryBuilder.class, resultBuilder);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        resultBuilder.toString());
  }

  @Test
  void multiMatchWithoutFields_generatesMultiMatchQuery() throws ExpressionNotAnalyzableException {
    // Test multi_match with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(builder, "multi_match", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(MultiMatchQueryBuilder.class, result);
    assertEquals(
        """
        {
          "multi_match" : {
            "query" : "Hi",
            "fields" : [ ],
            "type" : "best_fields",
            "operator" : "OR",
            "slop" : 0,
            "prefix_length" : 0,
            "max_expansions" : 50,
            "zero_terms_query" : "NONE",
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void simpleQueryStringWithoutFields_generatesSimpleQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    // Test simple_query_string with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "simple_query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(SimpleQueryStringBuilder.class, result);
    assertEquals(
        """
        {
          "simple_query_string" : {
            "query" : "Hi",
            "flags" : -1,
            "default_operator" : "or",
            "analyze_wildcard" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void queryStringWithoutFields_generatesQueryStringQuery()
      throws ExpressionNotAnalyzableException {
    // Test query_string with only query parameter (no fields)
    List<RexNode> arguments = List.of(aliasedStringLiteral);
    RexNode call =
        PPLFuncImpTable.INSTANCE.resolve(
            builder, "query_string", arguments.toArray(new RexNode[0]));
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(QueryStringQueryBuilder.class, result);
    assertEquals(
        """
        {
          "query_string" : {
            "query" : "Hi",
            "fields" : [ ],
            "type" : "best_fields",
            "default_operator" : "or",
            "max_determinized_states" : 10000,
            "enable_position_increments" : true,
            "fuzziness" : "AUTO",
            "fuzzy_prefix_length" : 0,
            "fuzzy_max_expansions" : 50,
            "phrase_slop" : 0,
            "escape" : false,
            "auto_generate_synonyms_phrase_query" : true,
            "fuzzy_transpositions" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void equals_generatesRangeQueryForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : "1987-02-03T04:34:56.000Z",
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notEquals_generatesBoolQueryForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "should" : [
              {
                "range" : {
                  "d" : {
                    "from" : "1987-02-03T04:34:56.000Z",
                    "to" : null,
                    "include_lower" : false,
                    "include_upper" : true,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              },
              {
                "range" : {
                  "d" : {
                    "from" : null,
                    "to" : "1987-02-03T04:34:56.000Z",
                    "include_lower" : true,
                    "include_upper" : false,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  /**
   * RexSimplify can strip the EXPR_TIMESTAMP UDT off a literal when a sibling clause is folded into
   * a Sarg (e.g. {@code @timestamp > X AND severityText IN (...)}), leaving the literal as plain
   * VARCHAR. The comparison must still emit a {@code format("date_time")} range query keyed off the
   * field's type so the shard's default date parser accepts the value.
   */
  @Test
  void gt_normalizesVarcharLiteralAgainstTimestampField() throws ExpressionNotAnalyzableException {
    RexLiteral varcharLiteral = (RexLiteral) builder.makeLiteral("1987-02-03 04:34:56");
    RexNode call = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, field4, varcharLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : null,
              "include_lower" : false,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  // Companion stripped-VARCHAR-literal tests for the remaining range shapes (equals -> gte+lte,
  // notEquals -> two-should bool, lte -> single range). Each must produce the same DSL as its
  // intact-UDT counterpart, proving the field-type fallback in isFieldOrLiteralDateTime keeps
  // ISO-8601 normalization + format("date_time") on every comparison op, not just gt. See #5481.
  @Test
  void equals_normalizesVarcharLiteralAgainstTimestampField()
      throws ExpressionNotAnalyzableException {
    RexLiteral varcharLiteral = (RexLiteral) builder.makeLiteral("1987-02-03 04:34:56");
    RexNode call = builder.makeCall(SqlStdOperatorTable.EQUALS, field4, varcharLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : "1987-02-03T04:34:56.000Z",
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notEquals_normalizesVarcharLiteralAgainstTimestampField()
      throws ExpressionNotAnalyzableException {
    RexLiteral varcharLiteral = (RexLiteral) builder.makeLiteral("1987-02-03 04:34:56");
    RexNode call = builder.makeCall(SqlStdOperatorTable.NOT_EQUALS, field4, varcharLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "should" : [
              {
                "range" : {
                  "d" : {
                    "from" : "1987-02-03T04:34:56.000Z",
                    "to" : null,
                    "include_lower" : false,
                    "include_upper" : true,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              },
              {
                "range" : {
                  "d" : {
                    "from" : null,
                    "to" : "1987-02-03T04:34:56.000Z",
                    "include_lower" : true,
                    "include_upper" : false,
                    "format" : "date_time",
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void lte_normalizesVarcharLiteralAgainstTimestampField() throws ExpressionNotAnalyzableException {
    RexLiteral varcharLiteral = (RexLiteral) builder.makeLiteral("1987-02-03 04:34:56");
    RexNode call = builder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, field4, varcharLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : null,
              "to" : "1987-02-03T04:34:56.000Z",
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void gte_generatesRangeQueryWithFormatForDateTime() throws ExpressionNotAnalyzableException {
    RexNode call =
        builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, field4, dateTimeLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(RangeQueryBuilder.class, result);
    assertEquals(
        """
        {
          "range" : {
            "d" : {
              "from" : "1987-02-03T04:34:56.000Z",
              "to" : null,
              "include_lower" : true,
              "include_upper" : true,
              "format" : "date_time",
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isTrue_booleanField_generatesTermQuery() throws ExpressionNotAnalyzableException {
    // IS_TRUE(boolean_field) should generate a term query with value true
    RexNode call = builder.makeCall(SqlStdOperatorTable.IS_TRUE, field5);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(TermQueryBuilder.class, result);
    assertEquals(
        """
        {
          "term" : {
            "e" : {
              "value" : true,
              "boost" : 1.0
            }
          }
        }\
        """,
        result.toString());
  }

  @Test
  void isTrue_booleanFieldCombinedWithOtherCondition_generatesCompoundQuery()
      throws ExpressionNotAnalyzableException {
    // IS_TRUE(boolean_field) AND other_condition
    RexNode isTrueCall = builder.makeCall(SqlStdOperatorTable.IS_TRUE, field5);
    RexNode equalsCall = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, isTrueCall, equalsCall);
    QueryBuilder result = PredicateAnalyzer.analyze(andCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "term" : {
                  "e" : {
                    "value" : true,
                    "boost" : 1.0
                  }
                }
              },
              {
                "term" : {
                  "a" : {
                    "value" : 12,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void search_complementedPointsWithNullAsFalse_generatesExistsAndNotInQuery()
      throws ExpressionNotAnalyzableException {
    // Simulates: a != 12 AND a != 13 AND isnotnull(a)
    // Calcite merges this into SEARCH($0, Sarg[...; NULL AS FALSE]) with complemented points
    Sarg<BigDecimal> sarg =
        Sarg.of(
            RexUnknownAs.FALSE,
            ImmutableRangeSet.<BigDecimal>builder()
                .add(Range.lessThan(BigDecimal.valueOf(12)))
                .add(Range.open(BigDecimal.valueOf(12), BigDecimal.valueOf(13)))
                .add(Range.greaterThan(BigDecimal.valueOf(13)))
                .build());
    RexNode sargLiteral =
        builder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));
    RexNode call = builder.makeCall(SqlStdOperatorTable.SEARCH, field1, sargLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must_not" : [
                    {
                      "terms" : {
                        "a" : [
                          12.0,
                          13.0
                        ],
                        "boost" : 1.0
                      }
                    }
                  ],
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              },
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void search_complementedPointsWithNullAsUnknown_generatesExistsAndNotInQuery()
      throws ExpressionNotAnalyzableException {
    // Simulates: a NOT IN (12, 13)
    // Calcite represents this as SEARCH($0, Sarg[...; NULL AS UNKNOWN]) with complemented points
    // SQL three-valued logic: NULL NOT IN (...) evaluates to UNKNOWN (not TRUE),
    // so null rows must be excluded.
    Sarg<BigDecimal> sarg =
        Sarg.of(
            RexUnknownAs.UNKNOWN,
            ImmutableRangeSet.<BigDecimal>builder()
                .add(Range.lessThan(BigDecimal.valueOf(12)))
                .add(Range.open(BigDecimal.valueOf(12), BigDecimal.valueOf(13)))
                .add(Range.greaterThan(BigDecimal.valueOf(13)))
                .build());
    RexNode sargLiteral =
        builder.makeSearchArgumentLiteral(sarg, typeFactory.createSqlType(SqlTypeName.DECIMAL));
    RexNode call = builder.makeCall(SqlStdOperatorTable.SEARCH, field1, sargLiteral);
    QueryBuilder result = PredicateAnalyzer.analyze(call, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "bool" : {
                  "must_not" : [
                    {
                      "terms" : {
                        "a" : [
                          12.0,
                          13.0
                        ],
                        "boost" : 1.0
                      }
                    }
                  ],
                  "adjust_pure_negative" : true,
                  "boost" : 1.0
                }
              },
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notLike_keywordField_isNotPushable() {
    // NOT(LIKE(field, pattern)) on a keyword field is not pushable because LIKE itself is not
    List<RexNode> arguments =
        Arrays.asList(field2, builder.makeLiteral("%Hi%"), builder.makeLiteral(true));
    RexNode likeCall =
        PPLFuncImpTable.INSTANCE.resolve(builder, "like", arguments.toArray(new RexNode[0]));
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, likeCall);
    assertThrows(
        ExpressionNotAnalyzableException.class,
        () -> PredicateAnalyzer.analyze(notCall, schema, fieldTypes));
  }

  @Test
  void notGreaterThan_generatesExistsAndMustNotRange() throws ExpressionNotAnalyzableException {
    // NOT(a > 12) should generate bool query with must(exists) + mustNot(range)
    RexNode gtCall = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, field1, numericLiteral);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, gtCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "must_not" : [
              {
                "range" : {
                  "a" : {
                    "from" : 12,
                    "to" : null,
                    "include_lower" : false,
                    "include_upper" : true,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notIsNotNull_generatesOnlyMustNotExists() throws ExpressionNotAnalyzableException {
    // NOT(IS_NOT_NULL(a)) = IS_NULL(a) should generate must_not(exists) WITHOUT an exists in must
    RexNode isNotNullCall = builder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, field1);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, isNotNullCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "exists" : {
                  "field" : "a",
                  "boost" : 1.0
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notIsTrue_generatesOnlyMustNotTerm() throws ExpressionNotAnalyzableException {
    // NOT(IS_TRUE(e)) should generate must_not(term(e, true)) WITHOUT an exists filter
    RexNode isTrueCall = builder.makeCall(SqlStdOperatorTable.IS_TRUE, field5);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, isTrueCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "e" : {
                    "value" : true,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void notIsFalse_generatesOnlyMustNotTerm() throws ExpressionNotAnalyzableException {
    // NOT(IS_FALSE(e)) should generate must_not(term(e, false)) WITHOUT an exists filter
    RexNode isFalseCall = builder.makeCall(SqlStdOperatorTable.IS_FALSE, field5);
    RexNode notCall = builder.makeCall(SqlStdOperatorTable.NOT, isFalseCall);
    QueryBuilder result = PredicateAnalyzer.analyze(notCall, schema, fieldTypes);

    assertInstanceOf(BoolQueryBuilder.class, result);
    assertEquals(
        """
        {
          "bool" : {
            "must_not" : [
              {
                "term" : {
                  "e" : {
                    "value" : false,
                    "boost" : 1.0
                  }
                }
              }
            ],
            "adjust_pure_negative" : true,
            "boost" : 1.0
          }
        }\
        """,
        result.toString());
  }

  @Test
  void andWithUnpushableLike_partiallyPushesRangeAndLeavesLikeAsResidual()
      throws ExpressionNotAnalyzableException {
    // field3 (c) is text without .keyword → LIKE throws PredicateAnalyzerException
    // field4 (d) is date → timestamp range should push as RangeQueryBuilder
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("c", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("d", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP))
            .add("e", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
            .add("f", typeFactory.createUDT(ExprUDT.EXPR_IP))
            .add("g", typeFactory.createSqlType(SqlTypeName.OTHER))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    RexInputRef field3 = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 2);
    RexNode likeCall =
        builder.makeCall(
            SqlStdOperatorTable.LIKE, field3, stringLiteral, builder.makeLiteral("\\"));
    RexNode rangeCall =
        builder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, field4, dateTimeLiteral);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, rangeCall, likeCall);

    QueryExpression expression =
        PredicateAnalyzer.analyzeExpression(andCall, schema, fieldTypes, rowType, cluster);

    // Partial: only the range is pushed, LIKE is residual (not a script)
    assertTrue(expression.isPartial(), "AND with unpushable LIKE must be partial");

    QueryBuilder result = expression.builder();
    assertInstanceOf(BoolQueryBuilder.class, result);
    BoolQueryBuilder boolQuery = (BoolQueryBuilder) result;
    assertEquals(1, boolQuery.must().size(), "Only the pushable range conjunct goes into must[]");

    // The pushed must clause should be the range query
    QueryBuilder firstMust = boolQuery.must().get(0);
    assertInstanceOf(RangeQueryBuilder.class, firstMust);
    RangeQueryBuilder rangeQuery = (RangeQueryBuilder) firstMust;
    assertEquals("d", rangeQuery.fieldName());

    // The LIKE should appear in unAnalyzableNodes (residual for Filter above scan)
    List<RexNode> residual = expression.getUnAnalyzableNodes();
    assertEquals(1, residual.size(), "Unpushable LIKE must surface as a residual conjunct");
    assertEquals(likeCall, residual.getFirst());

    // Critically: no script clause anywhere in the pushed query
    assertFalse(
        result.toString().contains("\"script\""), "The filter path must never emit a script query");
  }

  // ===== US-006 Pushability Rule Tests =====

  /**
   * Table-driven test asserting pushed-vs-residual for each predicate type. Pushable: term
   * equality, numeric range, date range, exists, match. Residual (not pushable): arithmetic
   * comparison, string function, regex, non-decomposable OR.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("pushabilityTestCases")
  void pushabilityRule_predicateClassification(
      String description, RexNode predicate, boolean expectPushable)
      throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("c", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("d", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP))
            .add("e", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
            .add("f", typeFactory.createUDT(ExprUDT.EXPR_IP))
            .add("g", typeFactory.createSqlType(SqlTypeName.OTHER))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    if (expectPushable) {
      // Should succeed without throwing
      QueryExpression result =
          PredicateAnalyzer.analyzeExpression(predicate, schema, fieldTypes, rowType, cluster);
      assertFalse(
          result.builder().toString().contains("\"script\""),
          description + ": pushed query must not contain a script clause");
    } else {
      // Should throw — the predicate is not index-accelerable
      assertThrows(
          ExpressionNotAnalyzableException.class,
          () ->
              PredicateAnalyzer.analyzeExpression(predicate, schema, fieldTypes, rowType, cluster),
          description + ": non-pushable predicate must throw ExpressionNotAnalyzableException");
    }
  }

  static Stream<Arguments> pushabilityTestCases() {
    OpenSearchTypeFactory tf = OpenSearchTypeFactory.TYPE_FACTORY;
    RexBuilder rb = new RexBuilder(tf);
    RexInputRef intField = rb.makeInputRef(tf.createSqlType(SqlTypeName.INTEGER), 0);
    RexInputRef textKeywordField = rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef textNoKeywordField = rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR), 2);
    RexInputRef dateField = rb.makeInputRef(tf.createUDT(ExprUDT.EXPR_TIMESTAMP), 3);
    RexLiteral intLit = rb.makeExactLiteral(new BigDecimal(42));
    RexLiteral strLit = rb.makeLiteral("hello");
    RexNode dateLit =
        rb.makeLiteral("2024-01-01 00:00:00", tf.createUDT(ExprUDT.EXPR_TIMESTAMP), true);

    // Pushable: term equality (field = literal)
    RexNode termEquality = rb.makeCall(SqlStdOperatorTable.EQUALS, intField, intLit);
    // Pushable: numeric range (field > literal)
    RexNode numericRange = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, intField, intLit);
    // Pushable: date range (field >= literal)
    RexNode dateRange = rb.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, dateField, dateLit);
    // Pushable: exists / IS NOT NULL
    RexNode existsCheck = rb.makeCall(SqlStdOperatorTable.IS_NOT_NULL, intField);
    // Pushable: match relevance function
    RexNode aliasedFieldForMatch =
        rb.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rb.makeLiteral("field"), textKeywordField);
    RexNode aliasedQueryForMatch =
        rb.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rb.makeLiteral("query"), strLit);
    RexNode matchCall =
        PPLFuncImpTable.INSTANCE.resolve(
            rb, "match", new RexNode[] {aliasedFieldForMatch, aliasedQueryForMatch});

    // Residual: arithmetic comparison (field + 1 > literal)
    RexNode arithmeticExpr =
        rb.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rb.makeCall(SqlStdOperatorTable.PLUS, intField, rb.makeExactLiteral(BigDecimal.ONE)),
            intLit);
    // Residual: string function (UPPER(field) = literal)
    RexNode stringFuncExpr =
        rb.makeCall(
            SqlStdOperatorTable.EQUALS,
            rb.makeCall(SqlStdOperatorTable.UPPER, textKeywordField),
            strLit);
    // Residual: regex / LIKE on text without keyword
    RexNode regexExpr =
        rb.makeCall(SqlStdOperatorTable.LIKE, textNoKeywordField, strLit, rb.makeLiteral("\\"));
    // Residual: non-decomposable OR (pushable OR non-pushable)
    RexNode nonDecomposableOr =
        rb.makeCall(
            SqlStdOperatorTable.OR,
            termEquality,
            rb.makeCall(
                SqlStdOperatorTable.EQUALS,
                rb.makeCall(SqlStdOperatorTable.UPPER, textKeywordField),
                strLit));
    // Pushable: range on keyword field (index-accelerable via term dictionary)
    RexNode rangeOnKeyword =
        rb.makeCall(SqlStdOperatorTable.GREATER_THAN, textKeywordField, strLit);
    // Residual: LIKE on keyword field (not on closed pushable list)
    RexNode likeOnKeyword =
        rb.makeCall(SqlStdOperatorTable.LIKE, textKeywordField, strLit, rb.makeLiteral("\\"));
    // Pushable: range on IP field (BKD-accelerated)
    RexInputRef ipField = rb.makeInputRef(tf.createUDT(ExprUDT.EXPR_IP), 5);
    RexLiteral ipLit = rb.makeLiteral("192.168.0.1");
    RexNode rangeOnIp = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, ipField, ipLit);
    // Residual: range on struct/object field (non-scalar, not comparable)
    RexInputRef structField = rb.makeInputRef(tf.createSqlType(SqlTypeName.OTHER), 6);
    RexNode rangeOnStruct = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, structField, strLit);

    return Stream.of(
        Arguments.of("term equality", termEquality, true),
        Arguments.of("numeric range", numericRange, true),
        Arguments.of("date range", dateRange, true),
        Arguments.of("exists (IS NOT NULL)", existsCheck, true),
        Arguments.of("match relevance function", matchCall, true),
        Arguments.of("arithmetic comparison", arithmeticExpr, false),
        Arguments.of("string function (UPPER)", stringFuncExpr, false),
        Arguments.of("regex/LIKE on text without keyword", regexExpr, false),
        Arguments.of("non-decomposable OR with non-pushable disjunct", nonDecomposableOr, false),
        Arguments.of("range on keyword field", rangeOnKeyword, true),
        Arguments.of("LIKE on keyword field", likeOnKeyword, false),
        Arguments.of("range on IP field", rangeOnIp, true),
        Arguments.of("range on struct/object field", rangeOnStruct, false));
  }

  /**
   * Asserts that NO query generated by the filter path ever contains a "script" clause, AND that
   * pushable cases produce a QueryBuilder while residual cases throw.
   */
  @ParameterizedTest(name = "noScriptInFilterPath: {0}")
  @MethodSource("noScriptTestCases")
  void filterPath_neverEmitsScriptQuery(
      String description, RexNode predicate, boolean expectPushable) {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("c", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("d", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP))
            .add("e", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
            .add("f", typeFactory.createUDT(ExprUDT.EXPR_IP))
            .add("g", typeFactory.createSqlType(SqlTypeName.OTHER))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    if (expectPushable) {
      // Must succeed and produce a non-script query
      try {
        QueryExpression result =
            PredicateAnalyzer.analyzeExpression(predicate, schema, fieldTypes, rowType, cluster);
        assertFalse(
            result.builder().toString().contains("\"script\""),
            description + ": pushed query must never contain a script clause");
      } catch (ExpressionNotAnalyzableException e) {
        fail(description + ": expected pushable but threw " + e.getMessage());
      }
    } else {
      // Must throw — the predicate is residual
      assertThrows(
          ExpressionNotAnalyzableException.class,
          () ->
              PredicateAnalyzer.analyzeExpression(predicate, schema, fieldTypes, rowType, cluster),
          description + ": residual predicate must throw ExpressionNotAnalyzableException");
    }
  }

  static Stream<Arguments> noScriptTestCases() {
    OpenSearchTypeFactory tf = OpenSearchTypeFactory.TYPE_FACTORY;
    RexBuilder rb = new RexBuilder(tf);
    RexInputRef intField = rb.makeInputRef(tf.createSqlType(SqlTypeName.INTEGER), 0);
    RexInputRef textKeywordField = rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR), 1);
    RexInputRef textNoKeywordField = rb.makeInputRef(tf.createSqlType(SqlTypeName.VARCHAR), 2);
    RexInputRef dateField = rb.makeInputRef(tf.createUDT(ExprUDT.EXPR_TIMESTAMP), 3);
    RexLiteral intLit = rb.makeExactLiteral(new BigDecimal(42));
    RexLiteral strLit = rb.makeLiteral("hello");
    RexNode dateLit =
        rb.makeLiteral("2024-01-01 00:00:00", tf.createUDT(ExprUDT.EXPR_TIMESTAMP), true);

    // Pushable cases
    RexNode termEquality = rb.makeCall(SqlStdOperatorTable.EQUALS, intField, intLit);
    RexNode numericRange = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, intField, intLit);
    RexNode dateRange = rb.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, dateField, dateLit);
    RexNode existsCheck = rb.makeCall(SqlStdOperatorTable.IS_NOT_NULL, intField);
    RexNode aliasedField =
        rb.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rb.makeLiteral("field"), textKeywordField);
    RexNode aliasedQuery =
        rb.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rb.makeLiteral("query"), strLit);
    RexNode matchCall =
        PPLFuncImpTable.INSTANCE.resolve(rb, "match", new RexNode[] {aliasedField, aliasedQuery});

    // Residual cases (previously-script-producing expressions)
    RexNode likeOnTextNoKeyword =
        rb.makeCall(SqlStdOperatorTable.LIKE, textNoKeywordField, strLit, rb.makeLiteral("\\"));
    RexNode equalsOnTextNoKeyword =
        rb.makeCall(SqlStdOperatorTable.EQUALS, textNoKeywordField, strLit);
    RexNode arithmeticExpr =
        rb.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rb.makeCall(SqlStdOperatorTable.PLUS, intField, rb.makeExactLiteral(BigDecimal.ONE)),
            intLit);
    RexNode stringFunc =
        rb.makeCall(
            SqlStdOperatorTable.EQUALS,
            rb.makeCall(SqlStdOperatorTable.UPPER, textKeywordField),
            strLit);
    // isEmpty: OR(IS_NULL, CHAR_LENGTH=0)
    RexNode isEmptyCall =
        PPLFuncImpTable.INSTANCE.resolve(rb, BuiltinFunctionName.IS_EMPTY, textKeywordField);
    // Non-decomposable OR
    RexNode nonDecomposableOr = rb.makeCall(SqlStdOperatorTable.OR, termEquality, stringFunc);
    // Pushable: range on keyword field (index-accelerable via term dictionary)
    RexNode rangeOnKeyword =
        rb.makeCall(SqlStdOperatorTable.GREATER_THAN, textKeywordField, strLit);
    // Residual: LIKE on keyword field (not on closed pushable list)
    RexNode likeOnKeyword =
        rb.makeCall(SqlStdOperatorTable.LIKE, textKeywordField, strLit, rb.makeLiteral("\\"));
    // Pushable: range on IP field (BKD-accelerated)
    RexInputRef ipField = rb.makeInputRef(tf.createUDT(ExprUDT.EXPR_IP), 5);
    RexLiteral ipLit = rb.makeLiteral("192.168.0.1");
    RexNode rangeOnIp = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, ipField, ipLit);
    // Residual: range on struct/object field (non-scalar, not comparable)
    RexInputRef structField = rb.makeInputRef(tf.createSqlType(SqlTypeName.OTHER), 6);
    RexNode rangeOnStruct = rb.makeCall(SqlStdOperatorTable.GREATER_THAN, structField, strLit);

    return Stream.of(
        Arguments.of("term equality", termEquality, true),
        Arguments.of("numeric range", numericRange, true),
        Arguments.of("date range", dateRange, true),
        Arguments.of("exists", existsCheck, true),
        Arguments.of("match", matchCall, true),
        Arguments.of("LIKE on text without keyword (residual)", likeOnTextNoKeyword, false),
        Arguments.of("equals on text without keyword (residual)", equalsOnTextNoKeyword, false),
        Arguments.of("arithmetic comparison (residual)", arithmeticExpr, false),
        Arguments.of("string function UPPER (residual)", stringFunc, false),
        Arguments.of("isEmpty OR (residual)", isEmptyCall, false),
        Arguments.of("non-decomposable OR (residual)", nonDecomposableOr, false),
        Arguments.of("range on keyword field", rangeOnKeyword, true),
        Arguments.of("LIKE on keyword field (residual)", likeOnKeyword, false),
        Arguments.of("range on IP field", rangeOnIp, true),
        Arguments.of("range on struct/object field (residual)", rangeOnStruct, false));
  }

  /**
   * Asserts that a mixed AND (pushable AND non-pushable) results in a partial expression where the
   * pushable conjuncts are pushed and non-pushable conjuncts remain as residual.
   */
  @Test
  void mixedAnd_producesPartialWithResidualFilter() throws ExpressionNotAnalyzableException {
    final RelDataType rowType =
        builder
            .getTypeFactory()
            .builder()
            .kind(StructKind.FULLY_QUALIFIED)
            .add("a", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("b", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("c", typeFactory.createSqlType(SqlTypeName.VARCHAR))
            .add("d", typeFactory.createUDT(ExprUDT.EXPR_TIMESTAMP))
            .add("e", typeFactory.createSqlType(SqlTypeName.BOOLEAN))
            .add("f", typeFactory.createUDT(ExprUDT.EXPR_IP))
            .add("g", typeFactory.createSqlType(SqlTypeName.OTHER))
            .build();
    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(0L));

    // Pushable: a = 42
    RexNode pushable = builder.makeCall(SqlStdOperatorTable.EQUALS, field1, numericLiteral);
    // Non-pushable: UPPER(b) = 'Hi'
    RexNode nonPushable =
        builder.makeCall(
            SqlStdOperatorTable.EQUALS,
            builder.makeCall(SqlStdOperatorTable.UPPER, field2),
            stringLiteral);
    RexNode andCall = builder.makeCall(SqlStdOperatorTable.AND, pushable, nonPushable);

    QueryExpression result =
        PredicateAnalyzer.analyzeExpression(andCall, schema, fieldTypes, rowType, cluster);

    // Must be partial
    assertTrue(result.isPartial(), "AND with non-pushable conjunct must be partial");

    // Only the pushable conjunct is in the query
    QueryBuilder qb = result.builder();
    assertInstanceOf(BoolQueryBuilder.class, qb);
    BoolQueryBuilder boolQb = (BoolQueryBuilder) qb;
    assertEquals(1, boolQb.must().size(), "Only pushable conjunct goes to must[]");
    assertInstanceOf(TermQueryBuilder.class, boolQb.must().get(0));

    // The non-pushable conjunct is in unAnalyzableNodes (becomes residual Filter)
    List<RexNode> residual = result.getUnAnalyzableNodes();
    assertEquals(1, residual.size());
    assertEquals(nonPushable, residual.getFirst());

    // No script in the output
    assertFalse(qb.toString().contains("\"script\""), "No script clause in pushed query");
  }

  /**
   * Verifies the match_all default: when NO filter is pushed, the SearchSourceBuilder has a null
   * query, which OpenSearch treats as match_all. This is the correct behavior — omitting the query
   * clause is equivalent to match_all and avoids unnecessary serialization.
   */
  @Test
  void noFilterPushed_queryIsNullWhichMeansMatchAll() {
    // OpenSearchRequestBuilder starts with no query set (null).
    // OpenSearch interprets a missing "query" field as match_all.
    // This test documents that contract — no explicit match_all is needed.
    var sourceBuilder =
        new org.opensearch.search.builder.SearchSourceBuilder()
            .from(0)
            .timeout(org.opensearch.common.unit.TimeValue.timeValueMinutes(1L))
            .trackScores(false);
    // Confirm the default state: query is null (equivalent to match_all in OpenSearch)
    assertEquals(
        null,
        sourceBuilder.query(),
        "When no filter is pushed, query is null — OpenSearch treats this as match_all");
  }
}
