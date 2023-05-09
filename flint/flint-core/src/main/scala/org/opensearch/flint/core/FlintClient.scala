/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

trait FlintClient {

  def createIndex(indexName: String, metadata: FlintMetadata): Unit

  def exists(indexName: String): Boolean

  def getIndexMetadata(indexName: String): FlintMetadata

}
