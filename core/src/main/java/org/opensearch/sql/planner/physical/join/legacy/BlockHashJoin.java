/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.physical.join.legacy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Block-based Hash Join implementation
 */
public class BlockHashJoin<T> extends JoinAlgorithm<T> {

    /**
     * Use terms filter optimization or not
     */
    private final boolean isUseTermsFilterOptimization;

    public BlockHashJoin(PhysicalOperator<T> left,
                         PhysicalOperator<T> right,
                         JoinType type,
                         JoinCondition condition,
                         BlockSize blockSize,
                         boolean isUseTermsFilterOptimization) {
        super(left, right, type, condition, blockSize);

        this.isUseTermsFilterOptimization = isUseTermsFilterOptimization;
    }

    @Override
    public Cost estimate() {
        return new Cost();
    }

    @Override
    protected void reopenRight() throws Exception {
        /*
        Objects.requireNonNull(params, "Execute params is not set so unable to add extra filter");

        if (isUseTermsFilterOptimization) {
            params.add(ExecuteParams.ExecuteParamType.EXTRA_QUERY_FILTER, queryForPushedDownOnConds());
        }
        */
        right.open(params);
    }

    @Override
    protected List<CombinedRow<T>> probe() {
        List<CombinedRow<T>> combinedRows = new ArrayList<>();
        int totalSize = 0;

        /* Return if already found enough matched rows to give ResourceMgr a chance to check resource usage */
        while (right.hasNext() && totalSize < hashTable.size()) {
            Row<T> rightRow = right.next();
            Collection<Row<T>> matchedLeftRows = hashTable.match(rightRow);

            if (!matchedLeftRows.isEmpty()) {
                combinedRows.add(new CombinedRow<>(rightRow, matchedLeftRows));
                totalSize += matchedLeftRows.size();
            }
        }
        return combinedRows;
    }

    /**
     * Build query for pushed down conditions in ON
     */
    // TODO: core module cannot depend on opensearch module
    /*
    private BoolQueryBuilder queryForPushedDownOnConds() {
        BoolQueryBuilder orQuery = boolQuery();
        Map<String, Collection<Object>>[] rightNameToLeftValuesGroup = hashTable.rightFieldWithLeftValues();

        for (Map<String, Collection<Object>> rightNameToLeftValues : rightNameToLeftValuesGroup) {
            if (LOG.isTraceEnabled()) {
                rightNameToLeftValues.forEach((rightName, leftValues) ->
                        LOG.trace("Right name to left values mapping: {} => {}", rightName, leftValues));
            }

            BoolQueryBuilder andQuery = boolQuery();
            rightNameToLeftValues.forEach(
                    (rightName, leftValues) -> andQuery.must(termsQuery(rightName, leftValues))
            );

            if (LOG.isTraceEnabled()) {
                LOG.trace("Terms filter optimization: {}", Strings.toString(andQuery));
            }
            orQuery.should(andQuery);
        }
        return orQuery;
    }
    */

    /*********************************************
     *          Getters for Explain
     *********************************************/

    public boolean isUseTermsFilterOptimization() {
        return isUseTermsFilterOptimization;
    }
}
