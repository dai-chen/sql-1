/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;

public class CalciteExecAggregatorTest {

  @Test
  void janinoCompilationWorksInPluginClassloaderContext() throws Exception {
    // This directly proves that Janino/Calcite compilation works with the correct classloader
    // setup, same pattern used by CalciteScriptEngine and by CalciteExecAggregator's probe
    String result = CalciteClassLoaderHelper.withCalciteClassLoader(() -> {
      String code =
          "public Object[] apply(Object root0) {\n"
          + "  return new Object[] { Integer.valueOf(40 + 2) };\n"
          + "}\n";
      org.apache.calcite.rex.RexExecutable executable =
          new org.apache.calcite.rex.RexExecutable(code, "test probe");
      org.apache.calcite.linq4j.function.Function1 fn = executable.getFunction();
      Object[] out = (Object[]) fn.apply(null);
      return "OK:" + out[0];
    }, CalciteExecAggregatorTest.class);
    assertEquals("OK:42", result);
  }

  @Test
  void aggregatorCollectsDocValuesAndProbes() throws Exception {
    // Build a small in-memory Lucene index with numeric and keyword doc values
    ByteBuffersDirectory dir = new ByteBuffersDirectory();
    IndexWriterConfig config = new IndexWriterConfig();
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      Document doc1 = new Document();
      doc1.add(new NumericDocValuesField("age", 25));
      doc1.add(new SortedDocValuesField("name", new BytesRef("alice")));
      writer.addDocument(doc1);

      Document doc2 = new Document();
      doc2.add(new NumericDocValuesField("age", 30));
      doc2.add(new SortedDocValuesField("name", new BytesRef("bob")));
      writer.addDocument(doc2);
    }

    // Create a minimal mock SearchContext with BigArrays
    SearchContext searchContext = mock(SearchContext.class);
    BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(org.opensearch.common.settings.Settings.EMPTY), new NoneCircuitBreakerService());
    when(searchContext.bigArrays()).thenReturn(bigArrays);
    doNothing().when(searchContext).addReleasable(any());
    SearchShardTarget shardTarget = mock(SearchShardTarget.class);
    when(searchContext.shardTarget()).thenReturn(shardTarget);

    // Instantiate the aggregator
    CalciteExecAggregator agg = new CalciteExecAggregator(
        "test_agg", List.of("age", "name"), true, searchContext, null, Map.of());

    // Read the index and feed docs through the aggregator's leaf collector
    try (DirectoryReader reader = DirectoryReader.open(dir)) {
      assertEquals(1, reader.leaves().size());
      LeafReaderContext leafCtx = reader.leaves().get(0);
      LeafBucketCollector collector = agg.getLeafCollector(leafCtx, LeafBucketCollector.NO_OP_COLLECTOR);
      collector.collect(0, 0);
      collector.collect(1, 0);
    }

    // Build the aggregation result
    InternalCalciteExec result = (InternalCalciteExec) agg.buildAggregation(0);

    // Verify doc values were collected
    assertNotNull(result);
    assertEquals(2, result.getRows().size());
    assertEquals(25L, result.getRows().get(0)[0]);
    assertEquals("alice", result.getRows().get(0)[1]);
    assertEquals(30L, result.getRows().get(1)[0]);
    assertEquals("bob", result.getRows().get(1)[1]);

    // Verify Janino probe succeeded
    assertNotNull(result.getProbeResult(), "probe_result should not be null");
    assertTrue(result.getProbeResult().startsWith("JANINO_COMPILE_OK"),
        "Expected JANINO_COMPILE_OK but got: " + result.getProbeResult());

    dir.close();
  }
}
