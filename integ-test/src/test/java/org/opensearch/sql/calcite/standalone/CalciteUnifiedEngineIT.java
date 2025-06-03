/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.unified.api.UnifiedQueryPlanner;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.sql.calcite.OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

public class CalciteUnifiedEngineIT extends CalcitePPLIntegTestCase {

    private UnifiedQueryPlanner planner;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        loadIndex(Index.BANK);

        SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
        rootSchema.add(OPEN_SEARCH_SCHEMA_NAME, new OpenSearchSchema(
                new OpenSearchRestClient(new CalcitePPLIntegTestCase.InternalRestHighLevelClient(client()))));
        this.planner = new UnifiedQueryPlanner(QueryType.PPL, rootSchema);
    }

    @Test
    public void plan() {
        Object test = planner.plan("source = OpenSearch." + TEST_INDEX_BANK);
        System.out.println(test);
    }

    class OpenSearchSchema extends AbstractSchema {

        private final StorageEngine osEngine;
        private final Map<String, Table> tableMap;

        public OpenSearchSchema(OpenSearchClient opensearchClient) {
            this.osEngine = new OpenSearchStorageEngine(opensearchClient, null);
            this.tableMap = new HashMap<>() {
                @Override
                public Table get(Object key) {
                    if (!super.containsKey(key)) {
                        QualifiedName fullName = new QualifiedName((String) key);
                        return (Table) osEngine.getTable(null, fullName.getSuffix());
                    } else {
                        return super.get(key);
                    }
                }
            };
        }

        @Override
        public Map<String, Table> getTableMap() {
            return tableMap;
        }
    }
}
