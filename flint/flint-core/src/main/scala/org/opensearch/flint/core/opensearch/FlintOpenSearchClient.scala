/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.opensearch

import org.apache.http.HttpHost
import org.opensearch.action.get.GetRequest
import org.opensearch.client.indices.{CreateIndexRequest, GetIndexRequest}
import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.opensearch.common.settings.Settings
import org.opensearch.flint.core.{FlintClient, FlintMetadata}

import scala.collection.JavaConverters._

class FlintOpenSearchClient extends FlintClient {

  override def createIndex(indexName: String, metadata: FlintMetadata): Unit = {
    withClient { client =>
      // Create an index request object
      val request = new CreateIndexRequest(indexName)

      // Create a settings object with _meta field
      /*
      val settings = Settings.builder()
        .put("_meta", metadata.meta)
        .build()

      // Add the settings to the index request object
      request.settings(settings)
       */

      val properties = Map(
        "properties" -> metadata.schema.map {
          case (field, fieldType) =>
            field -> Map("type" -> toOpenSearchType(fieldType)).asJava
        }.asJava,
        "_meta" -> Map("kind" -> "SkippingIndex").asJava //TODO: convert metadata.meta to Map
      ).asJava

      println(properties)
      request.mapping(properties)

      // Create the index with the mapping
      val response = client.indices().create(request, RequestOptions.DEFAULT)
    }
  }

  override def exists(indexName: String): Boolean = {
    withClient { client =>
      val request = new GetRequest(indexName)
      client.exists(request, RequestOptions.DEFAULT)
    }
  }

  override def getIndexMetadata(indexName: String): FlintMetadata = {
    withClient { client =>
      val request = new GetIndexRequest(indexName)
      val response = client.indices().get(request, RequestOptions.DEFAULT)

      //TODO: parse into flint metadata
      FlintMetadata(Map.empty, "")
    }
  }

  private def withClient[T](block: RestHighLevelClient => T): T = {
    val client = new RestHighLevelClient(
      RestClient.builder(new HttpHost("localhost", 9200, "http")))
    try {
      block(client)
    } finally {
      client.close()
    }
  }

  private def toOpenSearchType(flintType: String): String = {
    flintType match {
      case "int" => "integer"
      case "string" => "keyword"
      case "array<int>" => "integer"
    }
  }
}
