/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

/**
 * Immutable value type describing the coordinator-side combine for a staged Calcite execution. All
 * five modes defined by the design doc are implemented: CONCAT (US-002), MERGE_AGG (US-012), LIMIT
 * (US-013), TOP_N (US-014) and RANK_LIMIT (US-015).
 */
public class CombineDescriptor implements Writeable, ToXContentObject {

  /** The five combine modes defined by the design doc. */
  public enum Mode {
    CONCAT,
    MERGE_AGG,
    TOP_N,
    RANK_LIMIT,
    LIMIT
  }

  private final Mode mode;
  // Mode-specific payload kept generic: integer lists cover partitionKeys, groupKeys, orderKeys,
  // dirs; single ints cover k, n; string lists cover aggs, keys.
  private final List<Integer> intListParam;
  private final List<String> stringListParam;
  private final int intParam;
  // Single nullable string payload. Used by RANK_LIMIT to carry the ranking function name
  // (ROW_NUMBER, RANK or DENSE_RANK), which changes the reduce semantics and therefore cannot be
  // inferred on the reducing node. Null for every other mode.
  private final String stringParam;

  public CombineDescriptor(
      Mode mode, List<Integer> intListParam, List<String> stringListParam, int intParam) {
    this(mode, intListParam, stringListParam, intParam, null);
  }

  public CombineDescriptor(
      Mode mode,
      List<Integer> intListParam,
      List<String> stringListParam,
      int intParam,
      String stringParam) {
    this.mode = Objects.requireNonNull(mode);
    this.intListParam = intListParam == null ? List.of() : List.copyOf(intListParam);
    this.stringListParam = stringListParam == null ? List.of() : List.copyOf(stringListParam);
    this.intParam = intParam;
    this.stringParam = stringParam;
  }

  /** Convenience factory for CONCAT (no parameters). */
  public static CombineDescriptor concat() {
    return new CombineDescriptor(Mode.CONCAT, List.of(), List.of(), 0);
  }

  /** Factory for MERGE_AGG: partial aggregate on shard, merge (group+sum/min/max) on reduce. */
  public static CombineDescriptor mergeAgg(List<Integer> groupKeys, List<String> aggs) {
    return new CombineDescriptor(Mode.MERGE_AGG, groupKeys, aggs, 0);
  }

  /** Factory for LIMIT: shard-local Fetch(n) + coordinator truncate-at-n. */
  public static CombineDescriptor limit(int n) {
    return new CombineDescriptor(Mode.LIMIT, List.of(), List.of(), n);
  }

  /**
   * Factory for TOP_N (US-014): shard-local Sort+Fetch(n) + coordinator merge of the sorted runs.
   *
   * @param keys column indices of the sort keys, in collation order
   * @param dirs one direction spec per key, spelled {@code <Direction>:<NullDirection>} (e.g.
   *     {@code ASCENDING:LAST}); the null direction is always resolved to FIRST or LAST by the
   *     coordinator before it reaches the wire, never left UNSPECIFIED
   * @param n the fetch count
   */
  public static CombineDescriptor topN(List<Integer> keys, List<String> dirs, int n) {
    return new CombineDescriptor(Mode.TOP_N, keys, dirs, n);
  }

  /**
   * Default ranking function for {@link Mode#RANK_LIMIT}. It is the only one PPL can produce —
   * {@code dedup} lowers to {@code ROW_NUMBER} exclusively — so {@link #describe()} omits it and
   * renders the design doc's worked-example shape verbatim.
   */
  public static final String DEFAULT_RANK_FUNCTION = "ROW_NUMBER";

  /**
   * Factory for RANK_LIMIT (US-015, rule 3): the shard evaluates the window and its rank filter
   * locally and ships the surviving rows WITHOUT the rank column; the coordinator RECOMPUTES rank
   * over the union of shard outputs and re-applies the limit.
   *
   * @param partitionKeys column indices of the PARTITION BY keys, expressed in the SHIPPED row's
   *     index space (i.e. after the rank column has been dropped)
   * @param orderKeys one spec per ORDER BY key, spelled {@code
   *     <colIdx>:<Direction>:<NullDirection>} in the shipped row's index space; empty for an
   *     unordered window. The null direction is always resolved to FIRST or LAST before it reaches
   *     the wire, never left UNSPECIFIED.
   * @param k the inclusive rank bound, already normalized from {@code <=}/{@code <}/{@code =}
   * @param rankFunction ROW_NUMBER, RANK or DENSE_RANK — the reduce semantics differ per function
   */
  public static CombineDescriptor rankLimit(
      List<Integer> partitionKeys, List<String> orderKeys, int k, String rankFunction) {
    return new CombineDescriptor(Mode.RANK_LIMIT, partitionKeys, orderKeys, k, rankFunction);
  }

  public CombineDescriptor(StreamInput in) throws IOException {
    this.mode = in.readEnum(Mode.class);
    this.intListParam = in.readList(StreamInput::readVInt);
    this.stringListParam = in.readStringList();
    this.intParam = in.readVInt();
    this.stringParam = in.readOptionalString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeEnum(mode);
    out.writeCollection(intListParam, StreamOutput::writeVInt);
    out.writeStringCollection(stringListParam);
    out.writeVInt(intParam);
    out.writeOptionalString(stringParam);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();
    builder.field("mode", mode.name());
    // Emit mode-specific keys matching the design doc's camelCase spelling.
    switch (mode) {
      case MERGE_AGG:
        if (!intListParam.isEmpty()) {
          builder.field("groupKeys", intListParam);
        }
        if (!stringListParam.isEmpty()) {
          builder.field("aggs", stringListParam);
        }
        break;
      case TOP_N:
        if (!intListParam.isEmpty()) {
          builder.field("keys", intListParam);
        }
        if (!stringListParam.isEmpty()) {
          builder.field("dirs", stringListParam);
        }
        if (intParam > 0) {
          builder.field("n", intParam);
        }
        break;
      case RANK_LIMIT:
        if (!intListParam.isEmpty()) {
          builder.field("partitionKeys", intListParam);
        }
        if (!stringListParam.isEmpty()) {
          builder.field("orderKeys", stringListParam);
        }
        if (intParam > 0) {
          builder.field("k", intParam);
        }
        if (stringParam != null) {
          builder.field("rankFunction", stringParam);
        }
        break;
      case LIMIT:
        if (intParam > 0) {
          builder.field("n", intParam);
        }
        break;
      case CONCAT:
      default:
        // No extra keys for CONCAT.
        break;
    }
    builder.endObject();
    return builder;
  }

  /**
   * Parse a CombineDescriptor from XContent. Expects the parser to be positioned at START_OBJECT.
   */
  public static CombineDescriptor fromXContent(XContentParser parser) throws IOException {
    Mode mode = null;
    List<Integer> intList = List.of();
    List<String> strList = List.of();
    int intVal = 0;
    String strVal = null;

    String fieldName = null;
    XContentParser.Token token;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        fieldName = parser.currentName();
      } else if (token == XContentParser.Token.VALUE_STRING && "mode".equals(fieldName)) {
        mode = Mode.valueOf(parser.text().toUpperCase(Locale.ROOT));
      } else if (token == XContentParser.Token.VALUE_STRING && "rankFunction".equals(fieldName)) {
        strVal = parser.text();
      } else if (token == XContentParser.Token.VALUE_NUMBER) {
        if ("n".equals(fieldName) || "k".equals(fieldName)) {
          intVal = parser.intValue();
        }
      } else if (token == XContentParser.Token.START_ARRAY) {
        // Determine array type from field name. Integer-valued arrays are the positional column
        // indices (groupKeys, partitionKeys, keys); everything else is a string array.
        if ("groupKeys".equals(fieldName)
            || "partitionKeys".equals(fieldName)
            || "keys".equals(fieldName)) {
          java.util.ArrayList<Integer> ints = new java.util.ArrayList<>();
          while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            ints.add(parser.intValue());
          }
          intList = List.copyOf(ints);
        } else {
          java.util.ArrayList<String> strs = new java.util.ArrayList<>();
          while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            strs.add(parser.text());
          }
          strList = List.copyOf(strs);
        }
      }
    }
    if (mode == null) {
      throw new IllegalArgumentException("CombineDescriptor missing required 'mode' field");
    }
    return new CombineDescriptor(mode, intList, strList, intVal, strVal);
  }

  public Mode getMode() {
    return mode;
  }

  public List<Integer> getIntListParam() {
    return intListParam;
  }

  public List<String> getStringListParam() {
    return stringListParam;
  }

  public int getIntParam() {
    return intParam;
  }

  /** The nullable single-string payload. For RANK_LIMIT this is the ranking function name. */
  public String getStringParam() {
    return stringParam;
  }

  /**
   * The RANK_LIMIT ranking function, defaulting to {@link #DEFAULT_RANK_FUNCTION} when the wire
   * carried no explicit value. Never returns null, so the reduce side has no default to re-derive.
   */
  public String getRankFunction() {
    return stringParam == null ? DEFAULT_RANK_FUNCTION : stringParam;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CombineDescriptor that = (CombineDescriptor) o;
    return intParam == that.intParam
        && mode == that.mode
        && Objects.equals(intListParam, that.intListParam)
        && Objects.equals(stringListParam, that.stringListParam)
        && Objects.equals(stringParam, that.stringParam);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, intListParam, stringListParam, intParam, stringParam);
  }

  @Override
  public String toString() {
    return "CombineDescriptor{mode=" + mode + "}";
  }

  /**
   * Human-readable single-line rendering matching the design doc's shape: {@code CONCAT} for the
   * parameterless mode, and {@code MODE{k:v, ...}} for parameterized ones.
   */
  public String describe() {
    if (mode == Mode.CONCAT) {
      return "CONCAT";
    }
    StringBuilder sb = new StringBuilder(mode.name());
    sb.append('{');
    boolean first = true;
    switch (mode) {
      case MERGE_AGG:
        if (!intListParam.isEmpty()) {
          sb.append("groupKeys:").append(intListParam);
          first = false;
        }
        if (!stringListParam.isEmpty()) {
          if (!first) sb.append(", ");
          sb.append("aggs:").append(stringListParam);
        }
        break;
      case TOP_N:
        if (!intListParam.isEmpty()) {
          sb.append("keys:").append(intListParam);
          first = false;
        }
        if (!stringListParam.isEmpty()) {
          if (!first) sb.append(", ");
          sb.append("dirs:").append(stringListParam);
          first = false;
        }
        if (intParam > 0) {
          if (!first) sb.append(", ");
          sb.append("n:").append(intParam);
        }
        break;
      case RANK_LIMIT:
        if (!intListParam.isEmpty()) {
          sb.append("partitionKeys:").append(intListParam);
          first = false;
        }
        if (!stringListParam.isEmpty()) {
          if (!first) sb.append(", ");
          sb.append("orderKeys:").append(stringListParam);
          first = false;
        }
        if (intParam > 0) {
          if (!first) sb.append(", ");
          sb.append("k:").append(intParam);
          first = false;
        }
        // The ranking function is rendered ONLY when it is not the default. ROW_NUMBER is the only
        // function PPL produces, so the common rendering is the design doc's worked-example shape;
        // a RANK or DENSE_RANK window is always visible because it differs from the default.
        if (stringParam != null && !DEFAULT_RANK_FUNCTION.equals(stringParam)) {
          if (!first) sb.append(", ");
          sb.append("rankFunction:").append(stringParam);
        }
        break;
      case LIMIT:
        if (intParam > 0) {
          sb.append("n:").append(intParam);
        }
        break;
      default:
        break;
    }
    sb.append('}');
    return sb.toString();
  }
}
