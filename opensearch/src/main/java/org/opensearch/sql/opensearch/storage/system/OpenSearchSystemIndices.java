/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

/**
 * OpenSearch system index.
 */
@RequiredArgsConstructor
public class OpenSearchSystemIndices {

  public static final String NAME_PREFIX = ".plugins-ql-";

  private final Map<String, Map<String, Object>> systemIndices;

  private final OpenSearchClient client;

  /**
   * Construct by loading all system index mapping from json file.
   */
  public OpenSearchSystemIndices(OpenSearchClient client) {
    this.client = client;

    this.systemIndices = ImmutableMap.of(
        NAME_PREFIX + "external-tables", loadIndexMapping("system/mappings/external-tables.json")
    );
  }

  /**
   * Create all system indices if not exist.
   */
  public void createAllIndices() {
    systemIndices.forEach((indexName, indexMapping) -> {
      if (!client.exists(indexName)) {
        client.createIndex(indexName, indexMapping);
      }
    });
  }

  @SuppressWarnings("UnstableApiUsage")
  private Map<String, Object> loadIndexMapping(String path) {
    try {
      URL url = Resources.getResource(path);
      String content = Resources.toString(url, StandardCharsets.UTF_8);
      return XContentHelper.convertToMap(new BytesArray(content), false, XContentType.JSON).v2();
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read system index mapping from " + path, e);
    }
  }
}
