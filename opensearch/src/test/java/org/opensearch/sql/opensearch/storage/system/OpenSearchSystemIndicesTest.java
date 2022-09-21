/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.io.Resources;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchSystemIndicesTest {

  @Mock
  private OpenSearchClient client;

  @InjectMocks
  private OpenSearchSystemIndices systemIndices;

  @BeforeEach
  void setUp() {
    this.systemIndices = new OpenSearchSystemIndices(client);
  }

  @Test
  void createAllSystemIndices() {
    doNothing().when(client).createIndex(eq(".plugins-ql-external-tables"),
        argThat(mapping -> mapping.containsKey("mappings")));

    systemIndices.createAllIndices();
  }

  @Test
  void createOnlyIfNotExist() {
    when(client.exists(".plugins-ql-external-tables")).thenReturn(true);

    systemIndices.createAllIndices();
    verify(client, never())
        .createIndex(eq(".plugins-ql-external-tables"), any());
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  void errorWhenIndexMappingFileLoad() {
    try (MockedStatic<Resources> mock = mockStatic(Resources.class)) {
      mock.when(() -> Resources.toString(any(), any()))
          .thenThrow(IOException.class);
      assertThrows(IllegalStateException.class, () -> new OpenSearchSystemIndices(client));
    }
  }

}