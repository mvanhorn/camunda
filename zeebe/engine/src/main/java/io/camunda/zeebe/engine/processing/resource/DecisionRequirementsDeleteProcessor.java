/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.resource;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.camunda.search.filter.DecisionInstanceFilter;
import io.camunda.security.api.model.CamundaAuthentication;
import io.camunda.zeebe.engine.processing.distribution.CommandDistributionBehavior;
import io.camunda.zeebe.engine.processing.identity.AuthorizedTenants;
import io.camunda.zeebe.engine.processing.identity.authorization.AuthorizationCheckBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.exception.ForbiddenException;
import io.camunda.zeebe.engine.processing.identity.authorization.request.AuthorizationRequest;
import io.camunda.zeebe.engine.processing.resource.ResourceDeletionExceptions.NoSuchResourceException;
import io.camunda.zeebe.engine.processing.streamprocessor.DistributedTypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.deployment.DeployedDrg;
import io.camunda.zeebe.engine.state.deployment.PersistedDecision;
import io.camunda.zeebe.engine.state.distribution.DistributionQueue;
import io.camunda.zeebe.engine.state.immutable.DecisionState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.impl.record.value.batchoperation.BatchOperationCreationRecord;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DecisionRecord;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DecisionRequirementsRecord;
import io.camunda.zeebe.protocol.impl.record.value.history.HistoryDeletionRecord;
import io.camunda.zeebe.protocol.impl.record.value.resource.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.BatchOperationIntent;
import io.camunda.zeebe.protocol.record.intent.DecisionIntent;
import io.camunda.zeebe.protocol.record.intent.DecisionRequirementsIntent;
import io.camunda.zeebe.protocol.record.intent.HistoryDeletionIntent;
import io.camunda.zeebe.protocol.record.value.AuthorizationResourceType;
import io.camunda.zeebe.protocol.record.value.BatchOperationType;
import io.camunda.zeebe.protocol.record.value.HistoryDeletionType;
import io.camunda.zeebe.protocol.record.value.PermissionType;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.agrona.concurrent.UnsafeBuffer;

public class DecisionRequirementsDeleteProcessor
    implements DistributedTypedRecordProcessor<DecisionRequirementsRecord> {

  private final StateWriter stateWriter;
  private final TypedCommandWriter commandWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedResponseWriter responseWriter;
  private final KeyGenerator keyGenerator;
  private final DecisionState decisionState;
  private final AuthorizationCheckBehavior authCheckBehavior;
  private final CommandDistributionBehavior commandDistributionBehavior;
  private final TenantState tenantState;

  public DecisionRequirementsDeleteProcessor(
      final Writers writers,
      final KeyGenerator keyGenerator,
      final ProcessingState processingState,
      final AuthorizationCheckBehavior authCheckBehavior,
      final CommandDistributionBehavior commandDistributionBehavior) {
    stateWriter = writers.state();
    commandWriter = writers.command();
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
    this.keyGenerator = keyGenerator;
    decisionState = processingState.getDecisionState();
    this.authCheckBehavior = authCheckBehavior;
    this.commandDistributionBehavior = commandDistributionBehavior;
    tenantState = processingState.getTenantState();
  }

  @Override
  public void processNewCommand(final TypedRecord<DecisionRequirementsRecord> command) {
    final long drgKey = command.getValue().getDecisionRequirementsKey();
    final long eventKey = keyGenerator.nextKey();

    tryDeleteDrgs(command, drgKey, eventKey);

    commandDistributionBehavior
        .withKey(eventKey)
        .inQueue(DistributionQueue.DEPLOYMENT)
        .distribute(command);
    responseWriter.writeEventOnCommand(
        eventKey, DecisionRequirementsIntent.DELETED, command.getValue(), command);
  }

  @Override
  public void processDistributedCommand(final TypedRecord<DecisionRequirementsRecord> command) {
    final long drgKey = command.getValue().getDecisionRequirementsKey();
    final long eventKey = command.getKey();

    tryDeleteDrgs(command, drgKey, eventKey);

    commandDistributionBehavior.acknowledgeCommand(command);
  }

  @Override
  public ProcessingError tryHandleError(
      final TypedRecord<DecisionRequirementsRecord> command, final Throwable error) {
    if (error instanceof final ForbiddenException exception) {
      rejectionWriter.appendRejection(
          command, exception.getRejectionType(), exception.getMessage());
      responseWriter.writeRejectionOnCommand(
          command, exception.getRejectionType(), exception.getMessage());
      return ProcessingError.EXPECTED_ERROR;
    } else if (error instanceof final NoSuchResourceException exception) {
      rejectionWriter.appendRejection(command, RejectionType.NOT_FOUND, exception.getMessage());
      responseWriter.writeRejectionOnCommand(
          command, RejectionType.NOT_FOUND, exception.getMessage());

      if (command.isCommandDistributed()) {
        // If the command is distributed, and it cannot be found upon processing, we can acknowledge
        // the distribution.
        commandDistributionBehavior.acknowledgeCommand(command);
      }

      return ProcessingError.EXPECTED_ERROR;
    }

    return ProcessingError.UNEXPECTED_ERROR;
  }

  @VisibleForTesting
  void deleteDecisionRequirements(
      final DeployedDrg drg,
      final TypedRecord<ResourceDeletionRecord> command,
      final long eventKey) {
    doDeleteDecisionRequirements(
        drg,
        eventKey,
        command.isCommandDistributed(),
        command.getValue().isDeleteHistory(),
        command.getValue());
  }

  private void doDeleteDecisionRequirements(
      final DeployedDrg drg,
      final long eventKey,
      final boolean isDistributedCommand,
      final boolean isDeleteHistory,
      final ResourceDeletionRecord resourceDeletionRecord) {
    decisionState
        .findDecisionsByTenantAndDecisionRequirementsKey(
            drg.getTenantId(), drg.getDecisionRequirementsKey())
        .forEach(this::deleteDecision);

    if (!isDistributedCommand && isDeleteHistory && resourceDeletionRecord != null) {
      deleteDecisionInstanceHistory(
          drg.getDecisionRequirementsKey(), eventKey, resourceDeletionRecord);
    }

    final var drgRecord =
        new DecisionRequirementsRecord()
            .setDecisionRequirementsId(bufferAsString(drg.getDecisionRequirementsId()))
            .setDecisionRequirementsName(bufferAsString(drg.getDecisionRequirementsName()))
            .setDecisionRequirementsVersion(drg.getDecisionRequirementsVersion())
            .setDecisionRequirementsKey(drg.getDecisionRequirementsKey())
            .setResourceName(bufferAsString(drg.getResourceName()))
            .setChecksum(drg.getChecksum())
            .setResource(drg.getResource())
            .setTenantId(drg.getTenantId())
            .setDeploymentKey(drg.getDeploymentKey());

    stateWriter.appendFollowUpEvent(
        keyGenerator.nextKey(), DecisionRequirementsIntent.DELETED, drgRecord);
  }

  private void deleteDecision(final PersistedDecision persistedDecision) {
    final var decisionRecord =
        new DecisionRecord()
            .setDecisionId(bufferAsString(persistedDecision.getDecisionId()))
            .setDecisionName(bufferAsString(persistedDecision.getDecisionName()))
            .setVersion(persistedDecision.getVersion())
            .setVersionTag(persistedDecision.getVersionTag())
            .setDecisionKey(persistedDecision.getDecisionKey())
            .setDecisionRequirementsId(
                bufferAsString(persistedDecision.getDecisionRequirementsId()))
            .setDecisionRequirementsKey(persistedDecision.getDecisionRequirementsKey())
            .setTenantId(persistedDecision.getTenantId())
            .setDeploymentKey(persistedDecision.getDeploymentKey());

    stateWriter.appendFollowUpEvent(keyGenerator.nextKey(), DecisionIntent.DELETED, decisionRecord);
  }

  private void deleteDecisionInstanceHistory(
      final long decisionRequirementsKey,
      final long eventKey,
      final ResourceDeletionRecord resourceDeletionRecord) {
    final var filter =
        new DecisionInstanceFilter.Builder()
            .decisionRequirementsKeys(decisionRequirementsKey)
            .build();
    final long batchOperationKey = keyGenerator.nextKey();
    final var batchOperationRecord =
        new BatchOperationCreationRecord()
            .setBatchOperationKey(batchOperationKey)
            .setBatchOperationType(BatchOperationType.DELETE_DECISION_INSTANCE)
            .setEntityFilter(new UnsafeBuffer(MsgPackConverter.convertToMsgPack(filter)))
            .setAuthentication(
                new UnsafeBuffer(
                    MsgPackConverter.convertToMsgPack(CamundaAuthentication.anonymous())))
            .setFollowUpCommand(
                ValueType.HISTORY_DELETION,
                HistoryDeletionIntent.DELETE,
                new HistoryDeletionRecord()
                    .setResourceKey(decisionRequirementsKey)
                    .setResourceType(HistoryDeletionType.DECISION_REQUIREMENTS));
    commandWriter.appendFollowUpCommand(
        eventKey, BatchOperationIntent.CREATE, batchOperationRecord);

    resourceDeletionRecord.setBatchOperationKey(batchOperationKey);
    resourceDeletionRecord.setBatchOperationType(BatchOperationType.DELETE_DECISION_INSTANCE);
  }

  private void tryDeleteDrgs(
      final TypedRecord<DecisionRequirementsRecord> command,
      final long drgKey,
      final long eventKey) {
    final var deleted =
        untilDrgDeleted(
            command, tenantId -> tryFindAndDeleteDrg(command, drgKey, tenantId, eventKey));
    if (!deleted) {
      throw new NoSuchResourceException(drgKey);
    }
  }

  private boolean untilDrgDeleted(
      final TypedRecord<DecisionRequirementsRecord> command,
      final Function<String, Boolean> deletionCallback) {
    final String tenantId = command.getValue().getTenantId();
    if (!tenantId.isEmpty()) {
      return deletionCallback.apply(tenantId);
    }
    final var authorizedTenants = authCheckBehavior.getAuthorizedTenantIds(command);
    if (AuthorizedTenants.ANONYMOUS.equals(authorizedTenants)) {
      return Optional.of(deletionCallback.apply(TenantOwned.DEFAULT_TENANT_IDENTIFIER))
          .filter(Boolean::booleanValue)
          .orElseGet(() -> forEachTenantUntilDeleted(deletionCallback));
    }
    for (final var tenant : authorizedTenants.getAuthorizedTenantIds()) {
      if (deletionCallback.apply(tenant)) {
        return true;
      }
    }
    return false;
  }

  private boolean forEachTenantUntilDeleted(final Function<String, Boolean> deletionCallback) {
    final var deleted = new AtomicBoolean(false);
    tenantState.forEachTenant(
        tenant -> {
          deleted.set(deletionCallback.apply(tenant));
          return !deleted.get();
        });
    return deleted.get();
  }

  private boolean tryFindAndDeleteDrg(
      final TypedRecord<DecisionRequirementsRecord> command,
      final long drgKey,
      final String tenantId,
      final long eventKey) {
    final var drgOptional = decisionState.findDecisionRequirementsByTenantAndKey(tenantId, drgKey);
    if (drgOptional.isEmpty()) {
      return false;
    }
    final var drg = drgOptional.get();
    checkAuthorization(command, drg);
    doDeleteDecisionRequirements(drg, eventKey, command.isCommandDistributed(), false, null);
    return true;
  }

  private void checkAuthorization(
      final TypedRecord<DecisionRequirementsRecord> command, final DeployedDrg drg) {
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_DRD)
            .tenantId(drg.getTenantId())
            .addResourceId(bufferAsString(drg.getDecisionRequirementsId()))
            .build();
    if (authCheckBehavior.isAuthorizedOrInternalCommand(authRequest).isLeft()) {
      throw new ForbiddenException(authRequest);
    }
  }
}
