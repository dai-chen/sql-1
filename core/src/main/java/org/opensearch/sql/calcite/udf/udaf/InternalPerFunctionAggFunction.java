/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import org.opensearch.sql.calcite.udf.UserDefinedAggFunction;

public class InternalPerFunctionAggFunction
    implements UserDefinedAggFunction<InternalPerFunctionAggFunction.PerAccumulator> {

  @Override
  public PerAccumulator init() {
    return new PerAccumulator();
  }

  @Override
  public Object result(PerAccumulator accumulator) {
    return accumulator.value();
  }

  @Override
  public PerAccumulator add(PerAccumulator acc, Object... values) {
    if (values.length != 3) {
      throw new IllegalArgumentException(
          "internal_per_function requires exactly 3 arguments: field, unit, interval_seconds");
    }

    Object fieldValue = values[0];
    String unit = (String) values[1];
    Object intervalValue = values[2];

    if (fieldValue == null) {
      return acc; // Skip null values
    }

    // Convert field value to double
    double numericValue;
    if (fieldValue instanceof Number) {
      numericValue = ((Number) fieldValue).doubleValue();
    } else {
      throw new IllegalArgumentException(
          "Field value must be numeric, got: " + fieldValue.getClass());
    }

    // Add to sum
    acc.addValue(numericValue);

    // Set unit and interval if not already set
    if (acc.getUnit() == null) {
      acc.setUnit(unit);
    }
    if (acc.getIntervalSeconds() == null) {
      // Convert interval to long
      long intervalSeconds;
      if (intervalValue instanceof Number) {
        intervalSeconds = ((Number) intervalValue).longValue();
      } else {
        throw new IllegalArgumentException(
            "Interval seconds must be numeric, got: " + intervalValue.getClass());
      }
      acc.setIntervalSeconds(intervalSeconds);
    }

    return acc;
  }

  public static class PerAccumulator implements UserDefinedAggFunction.Accumulator {
    private double sum = 0.0;
    private String unit;
    private Long intervalSeconds;

    public PerAccumulator() {}

    @Override
    public Object value(Object... argList) {
      if (intervalSeconds == null || intervalSeconds <= 0) {
        return null; // No data or invalid interval
      }

      // Get unit conversion factor
      long unitFactor = getUnitFactor(unit);

      // Calculate rate: sum / interval_seconds * unit_factor
      double rate = (sum / intervalSeconds) * unitFactor;

      return rate;
    }

    public void addValue(double value) {
      sum += value;
    }

    public String getUnit() {
      return unit;
    }

    public void setUnit(String unit) {
      this.unit = unit;
    }

    public Long getIntervalSeconds() {
      return intervalSeconds;
    }

    public void setIntervalSeconds(Long intervalSeconds) {
      this.intervalSeconds = intervalSeconds;
    }

    private long getUnitFactor(String unit) {
      if (unit == null) {
        return 1; // Default to seconds
      }

      switch (unit.toLowerCase()) {
        case "s":
          return 1; // per second
        case "m":
          return 60; // per minute
        case "h":
          return 3600; // per hour
        case "d":
          return 86400; // per day
        default:
          throw new IllegalArgumentException(
              "Unsupported unit: " + unit + ". Supported units are: s, m, h, d");
      }
    }
  }
}
