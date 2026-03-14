/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class AnalyticsActionRequest extends ActionRequest {
  @Getter private final String plan;
  @Getter private final String queryType;

  public AnalyticsActionRequest(String plan, String queryType) {
    this.plan = plan;
    this.queryType = queryType;
  }

  public AnalyticsActionRequest(StreamInput in) throws IOException {
    super(in);
    plan = in.readString();
    queryType = in.readString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(plan);
    out.writeString(queryType);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
