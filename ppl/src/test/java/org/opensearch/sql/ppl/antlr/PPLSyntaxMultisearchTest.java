/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.antlr;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class PPLSyntaxMultisearchTest {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testMultisearchBasicSyntax() {
    ParseTree tree = new PPLSyntaxParser().parse(
        "source=accounts | multisearch [ source=accounts | where age > 30 ] [ source=accounts | where age < 25 ]"
    );
    assertNotEquals(null, tree);
  }

  @Test
  public void testMultisearchThreeSubsearches() {
    ParseTree tree = new PPLSyntaxParser().parse(
        "source=accounts | multisearch [ source=accounts | where age > 30 ] [ source=accounts | where age < 25 ] [ source=accounts | where age between 25 and 30 ]"
    );
    assertNotEquals(null, tree);
  }

  @Test
  public void testMultisearchWithComplexSubsearches() {
    ParseTree tree = new PPLSyntaxParser().parse(
        "source=accounts | multisearch [ source=accounts | where age > 30 | eval group = 'old' ] [ source=accounts | where age < 25 | eval group = 'young' ]"
    );
    assertNotEquals(null, tree);
  }

  @Test
  public void testMultisearchWithStats() {
    ParseTree tree = new PPLSyntaxParser().parse(
        "source=accounts | multisearch [ source=accounts | where age > 30 | stats count() by gender ] [ source=accounts | where age < 25 | stats count() by gender ]"
    );
    assertNotEquals(null, tree);
  }

  @Test
  public void testMultisearchOnlyOneSubsearch() {
    exceptionRule.expect(RuntimeException.class);
    new PPLSyntaxParser().parse("source=accounts | multisearch [ source=accounts | where age > 30 ]");
  }

  @Test
  public void testMultisearchEmptySubsearch() {
    exceptionRule.expect(RuntimeException.class);
    new PPLSyntaxParser().parse("source=accounts | multisearch [ ] [ source=accounts | where age > 30 ]");
  }
}
