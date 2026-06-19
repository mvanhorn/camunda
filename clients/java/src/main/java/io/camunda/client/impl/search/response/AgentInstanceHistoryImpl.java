/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.camunda.client.impl.search.response;

import io.camunda.client.api.search.enums.AgentInstanceHistoryCommitStatus;
import io.camunda.client.api.search.enums.AgentInstanceHistoryRole;
import io.camunda.client.api.search.response.AgentInstanceHistory;
import io.camunda.client.impl.util.EnumUtil;
import io.camunda.client.impl.util.ParseUtil;
import io.camunda.client.protocol.rest.AgentInstanceDocumentContent;
import io.camunda.client.protocol.rest.AgentInstanceHistoryItemMetrics;
import io.camunda.client.protocol.rest.AgentInstanceHistoryItemResult;
import io.camunda.client.protocol.rest.AgentInstanceMessageContent;
import io.camunda.client.protocol.rest.AgentInstanceObjectContent;
import io.camunda.client.protocol.rest.AgentInstanceTextContent;
import io.camunda.client.protocol.rest.AgentInstanceToolCall;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AgentInstanceHistoryImpl implements AgentInstanceHistory {

  private final long historyItemKey;
  private final long agentInstanceKey;
  private final long elementInstanceKey;
  private final long jobKey;
  private final String jobLease;
  private final Integer iteration;
  private final AgentInstanceHistoryRole role;
  private final List<Content> content;
  private final List<ToolCall> toolCalls;
  private final Metrics metrics;
  private final AgentInstanceHistoryCommitStatus commitStatus;
  private final OffsetDateTime producedAt;

  public AgentInstanceHistoryImpl(final AgentInstanceHistoryItemResult result) {
    historyItemKey = Long.parseLong(result.getHistoryItemKey());
    agentInstanceKey = Long.parseLong(result.getAgentInstanceKey());
    elementInstanceKey = Long.parseLong(result.getElementInstanceKey());
    jobKey = Long.parseLong(result.getJobKey());
    jobLease = result.getJobLease();
    iteration = result.getIteration();
    role = EnumUtil.convert(result.getRole(), AgentInstanceHistoryRole.class);
    content =
        result.getContent() != null
            ? result.getContent().stream()
                .map(AgentInstanceHistoryImpl::toContent)
                .collect(Collectors.toList())
            : Collections.emptyList();
    toolCalls =
        result.getToolCalls() != null
            ? result.getToolCalls().stream().map(ToolCallImpl::new).collect(Collectors.toList())
            : Collections.emptyList();
    metrics = new MetricsImpl(result.getMetrics());
    commitStatus =
        EnumUtil.convert(result.getCommitStatus(), AgentInstanceHistoryCommitStatus.class);
    producedAt = ParseUtil.parseOffsetDateTimeOrNull(result.getProducedAt());
  }

  @Override
  public long getHistoryItemKey() {
    return historyItemKey;
  }

  @Override
  public long getAgentInstanceKey() {
    return agentInstanceKey;
  }

  @Override
  public long getElementInstanceKey() {
    return elementInstanceKey;
  }

  @Override
  public long getJobKey() {
    return jobKey;
  }

  @Override
  public String getJobLease() {
    return jobLease;
  }

  @Override
  public Integer getIteration() {
    return iteration;
  }

  @Override
  public AgentInstanceHistoryRole getRole() {
    return role;
  }

  @Override
  public List<Content> getContent() {
    return content;
  }

  @Override
  public List<ToolCall> getToolCalls() {
    return toolCalls;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  @Override
  public AgentInstanceHistoryCommitStatus getCommitStatus() {
    return commitStatus;
  }

  @Override
  public OffsetDateTime getProducedAt() {
    return producedAt;
  }

  private static Content toContent(final AgentInstanceMessageContent proto) {
    if (proto instanceof AgentInstanceTextContent) {
      return new TextContentImpl((AgentInstanceTextContent) proto);
    } else if (proto instanceof AgentInstanceDocumentContent) {
      return new DocumentContentImpl((AgentInstanceDocumentContent) proto);
    } else if (proto instanceof AgentInstanceObjectContent) {
      return new ObjectContentImpl((AgentInstanceObjectContent) proto);
    }
    return new ContentImpl(proto);
  }

  private static class ContentImpl implements Content {

    private final String contentType;

    ContentImpl(final AgentInstanceMessageContent proto) {
      contentType = proto.getContentType();
    }

    @Override
    public String getContentType() {
      return contentType;
    }
  }

  private static class TextContentImpl extends ContentImpl implements TextContent {

    private final String text;

    TextContentImpl(final AgentInstanceTextContent proto) {
      super(proto);
      text = proto.getText();
    }

    @Override
    public String getText() {
      return text;
    }
  }

  private static class DocumentContentImpl extends ContentImpl implements DocumentContent {

    private final Object documentReference;

    DocumentContentImpl(final AgentInstanceDocumentContent proto) {
      super(proto);
      documentReference = proto.getDocumentReference();
    }

    @Override
    public Object getDocumentReference() {
      return documentReference;
    }
  }

  @SuppressWarnings("unchecked")
  private static class ObjectContentImpl extends ContentImpl implements ObjectContent {

    private final Map<String, Object> object;

    ObjectContentImpl(final AgentInstanceObjectContent proto) {
      super(proto);
      object = proto.getObject() != null ? (Map<String, Object>) proto.getObject() : null;
    }

    @Override
    public Map<String, Object> getObject() {
      return object;
    }
  }

  private static class ToolCallImpl implements ToolCall {

    private final String toolCallId;
    private final String toolName;
    private final String elementId;
    private final Map<String, Object> arguments;

    @SuppressWarnings("unchecked")
    ToolCallImpl(final AgentInstanceToolCall proto) {
      toolCallId = proto.getToolCallId();
      toolName = proto.getToolName();
      elementId = proto.getElementId();
      arguments = proto.getArguments() != null ? (Map<String, Object>) proto.getArguments() : null;
    }

    @Override
    public String getToolCallId() {
      return toolCallId;
    }

    @Override
    public String getToolName() {
      return toolName;
    }

    @Override
    public String getElementId() {
      return elementId;
    }

    @Override
    public Map<String, Object> getArguments() {
      return arguments;
    }
  }

  private static class MetricsImpl implements Metrics {

    private final long inputTokens;
    private final long outputTokens;
    private final long durationMs;

    MetricsImpl(final AgentInstanceHistoryItemMetrics proto) {
      inputTokens = proto.getInputTokens();
      outputTokens = proto.getOutputTokens();
      durationMs = proto.getDurationMs();
    }

    @Override
    public long getInputTokens() {
      return inputTokens;
    }

    @Override
    public long getOutputTokens() {
      return outputTokens;
    }

    @Override
    public long getDurationMs() {
      return durationMs;
    }
  }
}
