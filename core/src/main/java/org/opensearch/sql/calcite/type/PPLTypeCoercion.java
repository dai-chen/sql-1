/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercionImpl;

/**
 * PPL-specific type coercion extending Calcite's TypeCoercionImpl.
 * Adds DATE + TIME → TIMESTAMP coercion rule on top of Calcite defaults.
 * Registered via TypeCoercionFactory in UnifiedQueryContext.
 *
 * <p>Base conformance should be MYSQL_5 to get BOOLEAN↔NUMBER coercion for free.
 * Calcite's built-in rules already handle STRING→NUMERIC and DATE→TIMESTAMP widening.
 * This class only adds rules that Calcite doesn't provide natively.
 */
public class PPLTypeCoercion extends TypeCoercionImpl {

  public PPLTypeCoercion(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  /**
   * Override to add DATE + TIME → TIMESTAMP merge rule.
   * Calcite's default commonTypeForBinaryComparison handles DATE→TIMESTAMP widening
   * but does NOT handle merging DATE + TIME into TIMESTAMP.
   */
  @Override
  public RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2) {
    if (type1 == null || type2 == null) {
      return null;
    }
    SqlTypeName name1 = type1.getSqlTypeName();
    SqlTypeName name2 = type2.getSqlTypeName();
    // DATE + TIME → TIMESTAMP (either order)
    if ((name1 == SqlTypeName.DATE && name2 == SqlTypeName.TIME)
        || (name1 == SqlTypeName.TIME && name2 == SqlTypeName.DATE)) {
      return factory.createSqlType(SqlTypeName.TIMESTAMP);
    }
    return super.commonTypeForBinaryComparison(type1, type2);
  }
}
