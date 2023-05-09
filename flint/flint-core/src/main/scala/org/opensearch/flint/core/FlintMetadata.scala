/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core

//TODO: 1) define Flint schema type; 2) use builder pattern
case class FlintMetadata(schema: Map[String, String], meta: String) {

  def getSchema: Map[String, String] = schema

  def getContent: String = meta
}
