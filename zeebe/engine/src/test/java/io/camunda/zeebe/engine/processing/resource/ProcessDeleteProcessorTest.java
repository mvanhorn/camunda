/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.resource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.engine.metrics.ProcessDefinitionMetrics;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnBehaviors;
import io.camunda.zeebe.engine.processing.common.CatchEventBehavior;
import io.camunda.zeebe.engine.processing.common.ExpressionProcessor;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.camunda.zeebe.engine.processing.distribution.CommandDistributionBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.AuthorizationCheckBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.exception.ForbiddenException;
import io.camunda.zeebe.engine.processing.identity.authorization.request.AuthorizationRequest;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor.ProcessingError;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.deployment.DeployedProcess;
import io.camunda.zeebe.engine.state.immutable.BannedInstanceState;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.engine.state.immutable.TimerInstanceState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.camunda.zeebe.protocol.impl.record.value.resource.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.Either;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProcessDeleteProcessorTest {

  @Mock StateWriter stateWriter;
  @Mock TypedCommandWriter commandWriter;
  @Mock TypedRejectionWriter rejectionWriter;
  @Mock TypedResponseWriter responseWriter;
  @Mock KeyGenerator keyGenerator;
  @Mock ProcessState processState;
  @Mock ElementInstanceState elementInstanceState;
  @Mock TimerInstanceState timerInstanceState;
  @Mock BannedInstanceState bannedInstanceState;
  @Mock CatchEventBehavior catchEventBehavior;
  @Mock ExpressionProcessor expressionProcessor;
  @Mock AuthorizationCheckBehavior authCheckBehavior;
  @Mock ProcessDefinitionMetrics processDefinitionMetrics;
  @Mock ProcessingState processingState;
  @Mock BpmnBehaviors bpmnBehaviors;
  @Mock Writers writers;
  @Mock ExecutableProcess executableProcess;
  @Mock TenantState tenantState;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  CommandDistributionBehavior commandDistributionBehavior;

  ProcessDeleteProcessor processor;

  @BeforeEach
  void setUp() {
    when(writers.state()).thenReturn(stateWriter);
    when(writers.command()).thenReturn(commandWriter);
    when(writers.rejection()).thenReturn(rejectionWriter);
    when(writers.response()).thenReturn(responseWriter);
    when(processingState.getProcessState()).thenReturn(processState);
    when(processingState.getElementInstanceState()).thenReturn(elementInstanceState);
    when(processingState.getTimerState()).thenReturn(timerInstanceState);
    when(processingState.getBannedInstanceState()).thenReturn(bannedInstanceState);
    when(processingState.getTenantState()).thenReturn(tenantState);
    when(bpmnBehaviors.catchEventBehavior()).thenReturn(catchEventBehavior);
    when(bpmnBehaviors.expressionProcessor()).thenReturn(expressionProcessor);
    processor =
        new ProcessDeleteProcessor(
            writers,
            keyGenerator,
            processingState,
            bpmnBehaviors,
            authCheckBehavior,
            processDefinitionMetrics,
            commandDistributionBehavior);
  }

  @Test
  void shouldEmitDeletingAndDeletedEventsWhenNoActiveInstances() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long processKey = 42L;
    final var process = mockDeployedProcess(processKey, "myProcess", "tenant1", 1);
    setupNoActiveInstances(processKey);

    // when
    processor.deleteProcess(process, mockResourceDeletionCommand("tenant1"), 10L);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ProcessIntent.DELETING), any());
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ProcessIntent.DELETED), any());
    verify(processDefinitionMetrics).processDefinitionDeleted(processKey);
  }

  @Test
  void shouldThrowWhenActiveInstancesExist() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long processKey = 42L;
    final var process = mockDeployedProcess(processKey, "myProcess", "tenant1", 1);
    when(bannedInstanceState.getBannedProcessInstanceKeys()).thenReturn(List.of());
    when(elementInstanceState.hasActiveProcessInstances(eq(processKey), any())).thenReturn(true);

    // when / then
    assertThatThrownBy(
            () -> processor.deleteProcess(process, mockResourceDeletionCommand("tenant1"), 10L))
        .isInstanceOf(ResourceDeletionExceptions.ActiveProcessInstancesException.class);
    verify(stateWriter, never()).appendFollowUpEvent(anyLong(), eq(ProcessIntent.DELETED), any());
  }

  @Test
  void shouldThrowWhenProcessNotFoundInProcessNewCommand() {
    // given
    final long processKey = 99L;
    when(processState.getProcessByKeyAndTenant(eq(processKey), any())).thenReturn(null);
    final var command = mockProcessCommand(processKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ResourceDeletionExceptions.NoSuchResourceException.class);
  }

  @Test
  void shouldEmitDeletingAndDeletedEventsViaProcessNewCommand() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long processKey = 55L;
    final var process = mockDeployedProcess(processKey, "myProcess", "tenant1", 1);
    when(processState.getProcessByKeyAndTenant(eq(processKey), eq("tenant1"))).thenReturn(process);
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any(AuthorizationRequest.class)))
        .thenReturn(Either.right(null));
    setupNoActiveInstances(processKey);
    final var command = mockProcessCommand(processKey, "tenant1");

    // when
    processor.processNewCommand(command);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ProcessIntent.DELETING), any());
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ProcessIntent.DELETED), any());
    verify(processDefinitionMetrics).processDefinitionDeleted(processKey);
  }

  @Test
  void shouldRejectWithForbiddenWhenNotAuthorized() {
    // given
    final long processKey = 77L;
    final var idBuffer = new UnsafeBuffer("myProcess".getBytes());
    final var process = mock(DeployedProcess.class);
    when(process.getTenantId()).thenReturn("tenant1");
    when(process.getBpmnProcessId()).thenReturn(idBuffer);
    when(processState.getProcessByKeyAndTenant(eq(processKey), eq("tenant1"))).thenReturn(process);
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any(AuthorizationRequest.class)))
        .thenReturn(Either.left(null));
    final var command = mockProcessCommand(processKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnExpectedErrorForForbiddenException() {
    // given
    final var command = (TypedRecord<ProcessRecord>) mock(TypedRecord.class);
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(io.camunda.zeebe.protocol.record.value.AuthorizationResourceType.RESOURCE)
            .permissionType(io.camunda.zeebe.protocol.record.value.PermissionType.DELETE_PROCESS)
            .tenantId("tenant1")
            .addResourceId("myProcess")
            .build();
    final var exception = new ForbiddenException(authRequest);

    // when
    final var result = processor.tryHandleError(command, exception);

    // then
    org.assertj.core.api.Assertions.assertThat(result).isEqualTo(ProcessingError.EXPECTED_ERROR);
  }

  // --- helpers ---

  private DeployedProcess mockDeployedProcess(
      final long key, final String processId, final String tenantId, final int version) {
    final var process = mock(DeployedProcess.class);
    final var idBuffer = new UnsafeBuffer(processId.getBytes());
    when(process.getKey()).thenReturn(key);
    when(process.getBpmnProcessId()).thenReturn(idBuffer);
    when(process.getTenantId()).thenReturn(tenantId);
    when(process.getVersion()).thenReturn(version);
    when(process.getVersionTag()).thenReturn("");
    when(process.getResourceName()).thenReturn(new UnsafeBuffer(new byte[0]));
    when(process.getDeploymentKey()).thenReturn(1L);
    when(process.getProcess()).thenReturn(executableProcess);
    when(executableProcess.hasTimerStartEvent()).thenReturn(false);
    when(processState.getLatestProcessVersion(any(), eq(tenantId))).thenReturn(version);
    return process;
  }

  private void setupNoActiveInstances(final long processKey) {
    when(bannedInstanceState.getBannedProcessInstanceKeys()).thenReturn(List.of());
    when(elementInstanceState.hasActiveProcessInstances(eq(processKey), any())).thenReturn(false);
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<ResourceDeletionRecord> mockResourceDeletionCommand(final String tenantId) {
    final var command = (TypedRecord<ResourceDeletionRecord>) mock(TypedRecord.class);
    final var record = new ResourceDeletionRecord().setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    when(command.isCommandDistributed()).thenReturn(false);
    return command;
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<ProcessRecord> mockProcessCommand(final long key, final String tenantId) {
    final var command = (TypedRecord<ProcessRecord>) mock(TypedRecord.class);
    final var record = new ProcessRecord().setKey(key).setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    return command;
  }
}
