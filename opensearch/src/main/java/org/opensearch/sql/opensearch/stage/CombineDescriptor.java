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
 * Immutable value type describing the coordinator-side combine for a staged Calcite execution. Five
 * modes are defined; only CONCAT has real reduce behavior in this story (US-002). The other four
 * throw UnsupportedOperationException naming the implementing story.
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

  public CombineDescriptor(
      Mode mode, List<Integer> intListParam, List<String> stringListParam, int intParam) {
    this.mode = Objects.requireNonNull(mode);
    this.intListParam = intListParam == null ? List.of() : List.copyOf(intListParam);
    this.stringListParam = stringListParam == null ? List.of() : List.copyOf(stringListParam);
    this.intParam = intParam;
  }

  /** Convenience factory for CONCAT (no parameters). */
  public static CombineDescriptor concat() {
    return new CombineDescriptor(Mode.CONCAT, List.of(), List.of(), 0);
  }

  /** Factory for MERGE_AGG: partial aggregate on shard, merge (group+sum/min/max) on reduce. */
  public static CombineDescriptor mergeAgg(List<Integer> groupKeys, List<String> aggs) {
    return new CombineDescriptor(Mode.MERGE_AGG, groupKeys, aggs, 0);
  }

  public CombineDescriptor(StreamInput in) throws IOException {
    this.mode = in.readEnum(Mode.class);
    this.intListParam = in.readList(StreamInput::readVInt);
    this.stringListParam = in.readStringList();
    this.intParam = in.readVInt();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeEnum(mode);
    out.writeCollection(intListParam, StreamOutput::writeVInt);
    out.writeStringCollection(stringListParam);
    out.writeVInt(intParam);
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
        if (!stringListParam.isEmpty()) {
          builder.field("keys", stringListParam);
        }
        if (!intListParam.isEmpty()) {
          builder.field("dirs", intListParam);
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

    String fieldName = null;
    XContentParser.Token token;
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      if (token == XContentParser.Token.FIELD_NAME) {
        fieldName = parser.currentName();
      } else if (token == XContentParser.Token.VALUE_STRING && "mode".equals(fieldName)) {
        mode = Mode.valueOf(parser.text().toUpperCase(Locale.ROOT));
      } else if (token == XContentParser.Token.VALUE_NUMBER) {
        if ("n".equals(fieldName) || "k".equals(fieldName)) {
          intVal = parser.intValue();
        }
      } else if (token == XContentParser.Token.START_ARRAY) {
        // Determine array type from field name
        if ("groupKeys".equals(fieldName)
            || "partitionKeys".equals(fieldName)
            || "dirs".equals(fieldName)) {
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
    return new CombineDescriptor(mode, intList, strList, intVal);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CombineDescriptor that = (CombineDescriptor) o;
    return intParam == that.intParam
        && mode == that.mode
        && Objects.equals(intListParam, that.intListParam)
        && Objects.equals(stringListParam, that.stringListParam);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, intListParam, stringListParam, intParam);
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
        if (!stringListParam.isEmpty()) {
          sb.append("keys:").append(stringListParam);
          first = false;
        }
        if (!intListParam.isEmpty()) {
          if (!first) sb.append(", ");
          sb.append("dirs:").append(intListParam);
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
