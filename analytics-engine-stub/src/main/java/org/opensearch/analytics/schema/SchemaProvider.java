/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.schema.SchemaPlus;

/** Provides a Calcite {@link SchemaPlus} from the current cluster state. */
@FunctionalInterface
public interface SchemaProvider {

  /**
   * Builds a Calcite {@link SchemaPlus} from the given cluster state.
   *
   * @param clusterState the current cluster state (opaque Object to avoid server dependency)
   * @return a SchemaPlus with tables derived from index mappings
   */
  SchemaPlus buildSchema(Object clusterState);
}
