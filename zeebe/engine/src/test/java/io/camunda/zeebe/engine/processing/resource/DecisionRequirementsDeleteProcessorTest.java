/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import io.camunda.zeebe.engine.state.deployment.DeployedDrg;
import io.camunda.zeebe.engine.state.deployment.PersistedDecision;
import io.camunda.zeebe.engine.state.deployment.PersistedDecisionRequirements;
import io.camunda.zeebe.engine.state.immutable.DecisionState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DecisionRecord;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DecisionRequirementsRecord;
import io.camunda.zeebe.protocol.impl.record.value.resource.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.intent.BatchOperationIntent;
import io.camunda.zeebe.protocol.record.intent.DecisionIntent;
import io.camunda.zeebe.protocol.record.intent.DecisionRequirementsIntent;
import io.camunda.zeebe.protocol.record.value.AuthorizationResourceType;
import io.camunda.zeebe.protocol.record.value.PermissionType;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.Either;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DecisionRequirementsDeleteProcessorTest {

  @Mock StateWriter stateWriter;
  @Mock TypedCommandWriter commandWriter;
  @Mock TypedRejectionWriter rejectionWriter;
  @Mock TypedResponseWriter responseWriter;
  @Mock KeyGenerator keyGenerator;
  @Mock DecisionState decisionState;
  @Mock AuthorizationCheckBehavior authCheckBehavior;
  @Mock ProcessingState processingState;
  @Mock TenantState tenantState;
  @Mock Writers writers;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  CommandDistributionBehavior commandDistributionBehavior;

  DecisionRequirementsDeleteProcessor processor;

  @BeforeEach
  void setUp() {
    when(writers.state()).thenReturn(stateWriter);
    when(writers.command()).thenReturn(commandWriter);
    when(writers.rejection()).thenReturn(rejectionWriter);
    when(writers.response()).thenReturn(responseWriter);
    when(processingState.getDecisionState()).thenReturn(decisionState);
    when(processingState.getTenantState()).thenReturn(tenantState);
    processor =
        new DecisionRequirementsDeleteProcessor(
            writers, keyGenerator, processingState, authCheckBehavior, commandDistributionBehavior);
  }

  @Test
  void shouldEmitDeletedEventsForDrgAndAssociatedDecisions() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long drgKey = 42L;
    final var drg = buildDeployedDrg(drgKey, "myDrg", "tenant1");
    final var decision1 = buildPersistedDecision(10L, "decision1", drgKey, "tenant1");
    final var decision2 = buildPersistedDecision(11L, "decision2", drgKey, "tenant1");
    when(decisionState.findDecisionsByTenantAndDecisionRequirementsKey("tenant1", drgKey))
        .thenReturn(List.of(decision1, decision2));

    // when
    processor.deleteDecisionRequirements(drg, mockResourceDeletionCommand("tenant1", false), 10L);

    // then
    verify(stateWriter, times(2)).appendFollowUpEvent(anyLong(), eq(DecisionIntent.DELETED), any());
    verify(stateWriter, times(1))
        .appendFollowUpEvent(anyLong(), eq(DecisionRequirementsIntent.DELETED), any());
  }

  @Test
  void shouldThrowWhenDrgNotFoundInProcessNewCommand() {
    // given
    final long drgKey = 99L;
    when(decisionState.findDecisionRequirementsByTenantAndKey(any(), eq(drgKey)))
        .thenReturn(Optional.empty());
    final var command = mockDrgCommand(drgKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ResourceDeletionExceptions.NoSuchResourceException.class);
  }

  @Test
  void shouldEmitDeletedEventsViaProcessNewCommand() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long drgKey = 55L;
    final var drg = buildDeployedDrg(drgKey, "myDrg", "tenant1");
    when(decisionState.findDecisionRequirementsByTenantAndKey(eq("tenant1"), eq(drgKey)))
        .thenReturn(Optional.of(drg));
    when(decisionState.findDecisionsByTenantAndDecisionRequirementsKey("tenant1", drgKey))
        .thenReturn(List.of());
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.right(null));
    final var command = mockDrgCommand(drgKey, "tenant1");

    // when
    processor.processNewCommand(command);

    // then
    verify(stateWriter)
        .appendFollowUpEvent(anyLong(), eq(DecisionRequirementsIntent.DELETED), any());
  }

  @Test
  void shouldDeleteDecisionInstanceHistoryWhenFlagSet() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L, 200L, 300L);
    final long drgKey = 42L;
    final var drg = buildDeployedDrg(drgKey, "myDrg", "tenant1");
    when(decisionState.findDecisionsByTenantAndDecisionRequirementsKey("tenant1", drgKey))
        .thenReturn(List.of());
    final var command = mockResourceDeletionCommand("tenant1", false);
    command.getValue().setDeleteHistory(true);

    // when
    processor.deleteDecisionRequirements(drg, command, 10L);

    // then
    verify(commandWriter).appendFollowUpCommand(eq(10L), eq(BatchOperationIntent.CREATE), any());
  }

  @Test
  void shouldNotDeleteHistoryWhenDistributed() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L);
    final long drgKey = 42L;
    final var drg = buildDeployedDrg(drgKey, "myDrg", "tenant1");
    when(decisionState.findDecisionsByTenantAndDecisionRequirementsKey("tenant1", drgKey))
        .thenReturn(List.of());
    // command is distributed + deleteHistory=true
    final var command = mockResourceDeletionCommand("tenant1", true);
    command.getValue().setDeleteHistory(true);

    // when
    processor.deleteDecisionRequirements(drg, command, 10L);

    // then: history deletion must NOT be triggered for distributed commands
    verify(commandWriter, never())
        .appendFollowUpCommand(anyLong(), eq(BatchOperationIntent.CREATE), any());
  }

  @Test
  void shouldRejectWithForbiddenWhenNotAuthorized() {
    // given
    final long drgKey = 55L;
    final var drg = buildDeployedDrg(drgKey, "myDrg", "tenant1");
    when(decisionState.findDecisionRequirementsByTenantAndKey(eq("tenant1"), eq(drgKey)))
        .thenReturn(Optional.of(drg));
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.left(null));
    final var command = mockDrgCommand(drgKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnExpectedErrorForForbiddenException() {
    // given
    final var command = (TypedRecord<DecisionRequirementsRecord>) mock(TypedRecord.class);
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_DRD)
            .tenantId("tenant1")
            .addResourceId("myDrg")
            .build();
    final var exception = new ForbiddenException(authRequest);

    // when
    final var result = processor.tryHandleError(command, exception);

    // then
    assertThat(result).isEqualTo(ProcessingError.EXPECTED_ERROR);
  }

  // --- helpers ---

  private DeployedDrg buildDeployedDrg(
      final long drgKey, final String drgId, final String tenantId) {
    final var drgRecord =
        new DecisionRequirementsRecord()
            .setDecisionRequirementsKey(drgKey)
            .setDecisionRequirementsId(drgId)
            .setDecisionRequirementsName(drgId)
            .setDecisionRequirementsVersion(1)
            .setResourceName("resource.dmn")
            .setTenantId(tenantId)
            .setDeploymentKey(1L);
    final var persisted = new PersistedDecisionRequirements().wrap(drgRecord);
    return new DeployedDrg(null, persisted);
  }

  private PersistedDecision buildPersistedDecision(
      final long decisionKey, final String decisionId, final long drgKey, final String tenantId) {
    final var decisionRecord =
        new DecisionRecord()
            .setDecisionKey(decisionKey)
            .setDecisionId(decisionId)
            .setDecisionName(decisionId)
            .setVersion(1)
            .setDecisionRequirementsId("myDrg")
            .setDecisionRequirementsKey(drgKey)
            .setTenantId(tenantId)
            .setDeploymentKey(1L);
    return new PersistedDecision().wrap(decisionRecord);
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<ResourceDeletionRecord> mockResourceDeletionCommand(
      final String tenantId, final boolean distributed) {
    final var command = (TypedRecord<ResourceDeletionRecord>) mock(TypedRecord.class);
    final var record = new ResourceDeletionRecord().setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    when(command.isCommandDistributed()).thenReturn(distributed);
    return command;
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<DecisionRequirementsRecord> mockDrgCommand(
      final long drgKey, final String tenantId) {
    final var command = (TypedRecord<DecisionRequirementsRecord>) mock(TypedRecord.class);
    final var record =
        new DecisionRequirementsRecord().setDecisionRequirementsKey(drgKey).setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    return command;
  }
}
