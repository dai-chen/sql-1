/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class QueryCompleterTest {

  private Map<String, Table> tables;
  private LineReader lineReader;

  @Before
  public void setUp() throws Exception {
    tables = SampleDataLoader.loadFromClasspath("data/hr.json");
    lineReader = mock(LineReader.class);
  }

  private List<String> complete(QueryCompleter completer, String prefix) {
    ParsedLine parsedLine = mock(ParsedLine.class);
    when(parsedLine.word()).thenReturn(prefix);
    when(parsedLine.wordCursor()).thenReturn(prefix.length());
    when(parsedLine.line()).thenReturn(prefix);
    when(parsedLine.cursor()).thenReturn(prefix.length());
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(lineReader, parsedLine, candidates);
    return candidates.stream().map(Candidate::value).collect(Collectors.toList());
  }

  private List<String> complete(QueryCompleter completer, String fullLine, String currentWord) {
    ParsedLine parsedLine = mock(ParsedLine.class);
    when(parsedLine.line()).thenReturn(fullLine);
    when(parsedLine.cursor()).thenReturn(fullLine.length());
    when(parsedLine.word()).thenReturn(currentWord);
    when(parsedLine.wordCursor()).thenReturn(currentWord.length());
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(lineReader, parsedLine, candidates);
    return candidates.stream().map(Candidate::value).collect(Collectors.toList());
  }

  @Test
  public void testSqlKeywordCompletion() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "SEL");
    assertThat(results, hasItem("SELECT"));
  }

  @Test
  public void testPplKeywordCompletion() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results = complete(completer, "sou");
    assertThat(results, hasItem("source"));
  }

  @Test
  public void testTableNameCompletion() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "emp");
    assertThat(results, hasItem("employees"));
  }

  @Test
  public void testColumnNameCompletion() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "na");
    assertThat(results, hasItem("name"));
  }

  @Test
  public void testSqlKeywordsNotInPplMode() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results = complete(completer, "SEL");
    assertThat(results, not(hasItem("SELECT")));
  }

  @Test
  public void testPplKeywordsNotInSqlMode() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "sou");
    assertThat(results, not(hasItem("source")));
  }

  @Test
  public void testUpdateLanguageSwitchesKeywords() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    assertThat(complete(completer, "SEL"), hasItem("SELECT"));
    completer.updateLanguage(QueryType.PPL);
    assertThat(complete(completer, "SEL"), not(hasItem("SELECT")));
    assertThat(complete(completer, "sou"), hasItem("source"));
  }

  @Test
  public void testUpdateTablesUpdatesCompletions() {
    QueryCompleter completer = new QueryCompleter(Map.of(), QueryType.SQL);
    assertThat(complete(completer, "emp"), not(hasItem("employees")));
    completer.updateTables(tables);
    assertThat(complete(completer, "emp"), hasItem("employees"));
  }

  @Test
  public void testCaseInsensitiveMatching() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "sel");
    assertThat(results, hasItem("SELECT"));
  }

  @Test
  public void testEmptyPrefixReturnsAll() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "");
    assertThat(results, not(empty()));
  }

  @Test
  public void testSqlCompletesTableNamesAfterFrom() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "SELECT * FROM ", "");
    assertThat(results, hasItem("employees"));
    assertThat(results, hasItem("departments"));
    assertThat(results, not(hasItem("SELECT")));
    assertThat(results, not(hasItem("name")));
  }

  @Test
  public void testPplCompletesTableNamesAfterSource() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results = complete(completer, "source = ", "");
    assertThat(results, hasItem("catalog.employees"));
    assertThat(results, hasItem("catalog.departments"));
    assertThat(results, not(hasItem("source")));
  }

  @Test
  public void testSqlCompletesColumnNamesAfterWhere() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.SQL);
    List<String> results = complete(completer, "SELECT * FROM employees WHERE ", "");
    assertThat(results, hasItem("name"));
    assertThat(results, hasItem("salary"));
    assertThat(results, not(hasItem("SELECT")));
    assertThat(results, not(hasItem("employees")));
    assertThat(results, not(hasItem("budget")));
  }

  @Test
  public void testPplCompletesColumnNamesAfterPipeWhere() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results = complete(completer, "source = catalog.employees | where ", "");
    assertThat(results, hasItem("name"));
    assertThat(results, hasItem("salary"));
    assertThat(results, not(hasItem("source")));
    assertThat(results, not(hasItem("budget")));
  }

  @Test
  public void testPplCompletesColumnNamesAfterStats() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results =
        complete(completer, "source = catalog.employees | stats count() by ", "");
    assertThat(results, hasItem("department"));
    assertThat(results, hasItem("age"));
    assertThat(results, not(hasItem("budget")));
  }

  @Test
  public void testPplCompletesColumnNamesAfterFields() {
    QueryCompleter completer = new QueryCompleter(tables, QueryType.PPL);
    List<String> results = complete(completer, "source = catalog.employees | fields ", "");
    assertThat(results, hasItem("name"));
    assertThat(results, hasItem("id"));
    assertThat(results, not(hasItem("budget")));
  }
}
