/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.resource;

import static io.camunda.zeebe.engine.state.instance.TimerInstance.NO_ELEMENT_INSTANCE;
import static io.camunda.zeebe.protocol.ZbColumnFamilies.PROCESS_CACHE_BY_ID_AND_VERSION;
import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.camunda.search.filter.ProcessInstanceFilter;
import io.camunda.security.api.model.CamundaAuthentication;
import io.camunda.zeebe.engine.metrics.ProcessDefinitionMetrics;
import io.camunda.zeebe.engine.processing.bpmn.behavior.BpmnBehaviors;
import io.camunda.zeebe.engine.processing.common.CatchEventBehavior;
import io.camunda.zeebe.engine.processing.deployment.StartEventSubscriptionManager;
import io.camunda.zeebe.engine.processing.distribution.CommandDistributionBehavior;
import io.camunda.zeebe.engine.processing.identity.AuthorizedTenants;
import io.camunda.zeebe.engine.processing.identity.authorization.AuthorizationCheckBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.exception.ForbiddenException;
import io.camunda.zeebe.engine.processing.identity.authorization.request.AuthorizationRequest;
import io.camunda.zeebe.engine.processing.resource.ResourceDeletionExceptions.ActiveProcessInstancesException;
import io.camunda.zeebe.engine.processing.resource.ResourceDeletionExceptions.NoSuchResourceException;
import io.camunda.zeebe.engine.processing.streamprocessor.DistributedTypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.deployment.DeployedProcess;
import io.camunda.zeebe.engine.state.distribution.DistributionQueue;
import io.camunda.zeebe.engine.state.immutable.BannedInstanceState;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ProcessState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.engine.state.immutable.TimerInstanceState;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.impl.record.value.batchoperation.BatchOperationCreationRecord;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.camunda.zeebe.protocol.impl.record.value.history.HistoryDeletionRecord;
import io.camunda.zeebe.protocol.impl.record.value.resource.ResourceDeletionRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.BatchOperationIntent;
import io.camunda.zeebe.protocol.record.intent.HistoryDeletionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
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
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessDeleteProcessor implements DistributedTypedRecordProcessor<ProcessRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessDeleteProcessor.class);

  private final StateWriter stateWriter;
  private final TypedCommandWriter commandWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedResponseWriter responseWriter;
  private final KeyGenerator keyGenerator;
  private final ProcessState processState;
  private final ElementInstanceState elementInstanceState;
  private final TimerInstanceState timerInstanceState;
  private final BannedInstanceState bannedInstanceState;
  private final CatchEventBehavior catchEventBehavior;
  private final StartEventSubscriptions startEventSubscriptions;
  private final StartEventSubscriptionManager startEventSubscriptionManager;
  private final AuthorizationCheckBehavior authCheckBehavior;
  private final ProcessDefinitionMetrics processDefinitionMetrics;
  private final CommandDistributionBehavior commandDistributionBehavior;
  private final TenantState tenantState;

  public ProcessDeleteProcessor(
      final Writers writers,
      final KeyGenerator keyGenerator,
      final ProcessingState processingState,
      final BpmnBehaviors bpmnBehaviors,
      final AuthorizationCheckBehavior authCheckBehavior,
      final ProcessDefinitionMetrics processDefinitionMetrics,
      final CommandDistributionBehavior commandDistributionBehavior) {
    stateWriter = writers.state();
    commandWriter = writers.command();
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
    this.keyGenerator = keyGenerator;
    processState = processingState.getProcessState();
    elementInstanceState = processingState.getElementInstanceState();
    timerInstanceState = processingState.getTimerState();
    bannedInstanceState = processingState.getBannedInstanceState();
    catchEventBehavior = bpmnBehaviors.catchEventBehavior();
    startEventSubscriptionManager =
        new StartEventSubscriptionManager(processingState, keyGenerator, stateWriter);
    startEventSubscriptions =
        new StartEventSubscriptions(
            bpmnBehaviors.expressionProcessor(), catchEventBehavior, startEventSubscriptionManager);
    this.authCheckBehavior = authCheckBehavior;
    this.processDefinitionMetrics = processDefinitionMetrics;
    this.commandDistributionBehavior = commandDistributionBehavior;
    tenantState = processingState.getTenantState();
  }

  @Override
  public void processNewCommand(final TypedRecord<ProcessRecord> command) {
    final long processKey = command.getValue().getKey();
    final long eventKey = keyGenerator.nextKey();

    tryDeleteProcesses(command, processKey, eventKey);

    commandDistributionBehavior
        .withKey(eventKey)
        .inQueue(DistributionQueue.DEPLOYMENT)
        .distribute(command);
    responseWriter.writeEventOnCommand(
        eventKey, ProcessIntent.DELETED, command.getValue(), command);
  }

  @Override
  public void processDistributedCommand(final TypedRecord<ProcessRecord> command) {
    final long processKey = command.getValue().getKey();
    final long eventKey = command.getKey();

    tryDeleteProcesses(command, processKey, eventKey);

    commandDistributionBehavior.acknowledgeCommand(command);
  }

  @Override
  public ProcessingError tryHandleError(
      final TypedRecord<ProcessRecord> command, final Throwable error) {
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
        commandDistributionBehavior.acknowledgeCommand(command);
      }
      return ProcessingError.EXPECTED_ERROR;
    } else if (error instanceof final ActiveProcessInstancesException exception) {
      rejectionWriter.appendRejection(command, RejectionType.INVALID_STATE, exception.getMessage());
      responseWriter.writeRejectionOnCommand(
          command, RejectionType.INVALID_STATE, exception.getMessage());
      return ProcessingError.EXPECTED_ERROR;
    }
    return ProcessingError.UNEXPECTED_ERROR;
  }

  @VisibleForTesting
  void deleteProcess(
      final DeployedProcess process,
      final TypedRecord<ResourceDeletionRecord> command,
      final long eventKey) {
    doDeleteProcess(
        process,
        eventKey,
        command.isCommandDistributed(),
        command.getValue().isDeleteHistory(),
        command.getValue());
  }

  private void doDeleteProcess(
      final DeployedProcess process,
      final long eventKey,
      final boolean isDistributedCommand,
      final boolean isDeleteHistory,
      final ResourceDeletionRecord resourceDeletionRecord) {
    // We don't add the checksum or resource in this event. The checksum is not easily available
    // and the resources are left out to prevent exceeding the maximum batch size.
    final var processIdBuffer = process.getBpmnProcessId();
    final var tenantId = process.getTenantId();
    final var processRecord =
        new ProcessRecord()
            .setBpmnProcessId(processIdBuffer)
            .setVersion(process.getVersion())
            .setVersionTag(process.getVersionTag())
            .setKey(process.getKey())
            .setResourceName(process.getResourceName())
            .setTenantId(tenantId)
            .setDeploymentKey(process.getDeploymentKey());
    stateWriter.appendFollowUpEvent(keyGenerator.nextKey(), ProcessIntent.DELETING, processRecord);

    final String processId = processRecord.getBpmnProcessId();
    final var latestVersion = processState.getLatestProcessVersion(processId, tenantId);

    // If we are deleting the latest version we must unsubscribe the start events
    if (latestVersion == process.getVersion()) {
      unsubscribeStartEvents(process);

      final var previousVersion =
          processState.findProcessVersionBefore(processId, latestVersion, tenantId);
      // If there is a previous version we must resubscribe to the previous version's start events.
      if (previousVersion.isPresent()) {
        final var previousProcess =
            processState.getProcessByProcessIdAndVersion(
                processIdBuffer, previousVersion.get(), tenantId);
        if (previousProcess == null) {
          warnPreviousProcessNotFound(
              processIdBuffer, previousVersion.get(), tenantId, processId, latestVersion);
        } else {
          startEventSubscriptions.resubscribeToStartEvents(previousProcess);
        }
      }
    }

    final var bannedInstances = bannedInstanceState.getBannedProcessInstanceKeys();
    final var hasRunningInstances =
        elementInstanceState.hasActiveProcessInstances(process.getKey(), bannedInstances);

    if (!hasRunningInstances) {
      if (!isDistributedCommand && isDeleteHistory && resourceDeletionRecord != null) {
        deleteProcessInstanceHistory(process.getKey(), eventKey, resourceDeletionRecord);
      }
      stateWriter.appendFollowUpEvent(keyGenerator.nextKey(), ProcessIntent.DELETED, processRecord);
      processDefinitionMetrics.processDefinitionDeleted(process.getKey());
    } else {
      throw new ActiveProcessInstancesException(process.getKey());
    }
  }

  private void deleteProcessInstanceHistory(
      final long processDefinitionKey,
      final long eventKey,
      final ResourceDeletionRecord resourceDeletionRecord) {
    final var filter =
        new ProcessInstanceFilter.Builder().processDefinitionKeys(processDefinitionKey).build();
    final long batchOperationKey = keyGenerator.nextKey();
    final var batchOperationRecord =
        new BatchOperationCreationRecord()
            .setBatchOperationKey(batchOperationKey)
            .setBatchOperationType(BatchOperationType.DELETE_PROCESS_INSTANCE)
            .setEntityFilter(new UnsafeBuffer(MsgPackConverter.convertToMsgPack(filter)))
            .setAuthentication(
                new UnsafeBuffer(
                    MsgPackConverter.convertToMsgPack(CamundaAuthentication.anonymous())))
            .setFollowUpCommand(
                ValueType.HISTORY_DELETION,
                HistoryDeletionIntent.DELETE,
                new HistoryDeletionRecord()
                    .setResourceKey(processDefinitionKey)
                    .setResourceType(HistoryDeletionType.PROCESS_DEFINITION));
    commandWriter.appendFollowUpCommand(
        eventKey, BatchOperationIntent.CREATE, batchOperationRecord);

    resourceDeletionRecord.setBatchOperationKey(batchOperationKey);
    resourceDeletionRecord.setBatchOperationType(BatchOperationType.DELETE_PROCESS_INSTANCE);
  }

  private void warnPreviousProcessNotFound(
      final DirectBuffer processIdBuffer,
      final int previousVersion,
      final String tenantId,
      final String processId,
      final int latestVersion) {
    LOG.warn(
        "Expected to find previous process: {} with version: {} and tenant: '{}' to "
            + "resubscribe start events, but no row exists in {}. "
            + "knownVersions: {}, current latest version: {}.",
        bufferAsString(processIdBuffer),
        previousVersion,
        tenantId,
        PROCESS_CACHE_BY_ID_AND_VERSION.name(),
        processState.getKnownProcessVersions(processId, tenantId),
        latestVersion);
  }

  private void unsubscribeStartEvents(final DeployedProcess deployedProcess) {
    final var process = deployedProcess.getProcess();
    if (process.hasTimerStartEvent()) {
      timerInstanceState.forEachTimerForElementInstance(
          NO_ELEMENT_INSTANCE,
          timer -> {
            if (timer.getProcessDefinitionKey() == deployedProcess.getKey()) {
              catchEventBehavior.unsubscribeFromTimerEvent(timer);
            }
          });
    }

    startEventSubscriptionManager.closeStartEventSubscriptions(deployedProcess);
  }

  private void tryDeleteProcesses(
      final TypedRecord<ProcessRecord> command, final long processKey, final long eventKey) {
    final var deleted =
        untilProcessDeleted(
            command, tenantId -> tryFindAndDeleteProcess(command, processKey, tenantId, eventKey));
    if (!deleted) {
      throw new NoSuchResourceException(processKey);
    }
  }

  private boolean untilProcessDeleted(
      final TypedRecord<ProcessRecord> command, final Function<String, Boolean> deletionCallback) {
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

  private boolean tryFindAndDeleteProcess(
      final TypedRecord<ProcessRecord> command,
      final long resourceKey,
      final String tenantId,
      final long eventKey) {
    final var process = processState.getProcessByKeyAndTenant(resourceKey, tenantId);
    if (process == null) {
      return false;
    }
    checkAuthorization(command, process);
    doDeleteProcess(process, eventKey, command.isCommandDistributed(), false, null);
    return true;
  }

  private void checkAuthorization(
      final TypedRecord<ProcessRecord> command, final DeployedProcess process) {
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_PROCESS)
            .tenantId(process.getTenantId())
            .addResourceId(bufferAsString(process.getBpmnProcessId()))
            .build();
    if (authCheckBehavior.isAuthorizedOrInternalCommand(authRequest).isLeft()) {
      throw new ForbiddenException(authRequest);
    }
  }
}
