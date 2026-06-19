/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.resource;

import static io.camunda.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.camunda.zeebe.engine.processing.distribution.CommandDistributionBehavior;
import io.camunda.zeebe.engine.processing.identity.AuthorizedTenants;
import io.camunda.zeebe.engine.processing.identity.authorization.AuthorizationCheckBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.exception.ForbiddenException;
import io.camunda.zeebe.engine.processing.identity.authorization.request.AuthorizationRequest;
import io.camunda.zeebe.engine.processing.resource.ResourceDeletionExceptions.NoSuchResourceException;
import io.camunda.zeebe.engine.processing.streamprocessor.DistributedTypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.deployment.PersistedResource;
import io.camunda.zeebe.engine.state.distribution.DistributionQueue;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.ResourceState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ResourceRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.ResourceIntent;
import io.camunda.zeebe.protocol.record.value.AuthorizationResourceType;
import io.camunda.zeebe.protocol.record.value.PermissionType;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class ResourceDeleteProcessor implements DistributedTypedRecordProcessor<ResourceRecord> {

  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedResponseWriter responseWriter;
  private final KeyGenerator keyGenerator;
  private final ResourceState resourceState;
  private final AuthorizationCheckBehavior authCheckBehavior;
  private final CommandDistributionBehavior commandDistributionBehavior;
  private final TenantState tenantState;

  public ResourceDeleteProcessor(
      final Writers writers,
      final KeyGenerator keyGenerator,
      final ProcessingState processingState,
      final AuthorizationCheckBehavior authCheckBehavior,
      final CommandDistributionBehavior commandDistributionBehavior) {
    stateWriter = writers.state();
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
    this.keyGenerator = keyGenerator;
    resourceState = processingState.getResourceState();
    this.authCheckBehavior = authCheckBehavior;
    this.commandDistributionBehavior = commandDistributionBehavior;
    tenantState = processingState.getTenantState();
  }

  @Override
  public void processNewCommand(final TypedRecord<ResourceRecord> command) {
    final long resourceKey = command.getValue().getResourceKey();
    final long eventKey = keyGenerator.nextKey();

    tryDeleteResources(command, resourceKey);

    commandDistributionBehavior
        .withKey(eventKey)
        .inQueue(DistributionQueue.DEPLOYMENT)
        .distribute(command);
    responseWriter.writeEventOnCommand(
        eventKey, ResourceIntent.DELETED, command.getValue(), command);
  }

  @Override
  public void processDistributedCommand(final TypedRecord<ResourceRecord> command) {
    final long resourceKey = command.getValue().getResourceKey();

    tryDeleteResources(command, resourceKey);

    commandDistributionBehavior.acknowledgeCommand(command);
  }

  @Override
  public ProcessingError tryHandleError(
      final TypedRecord<ResourceRecord> command, final Throwable error) {
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
    }
    return ProcessingError.UNEXPECTED_ERROR;
  }

  @VisibleForTesting
  void deleteResource(final PersistedResource persistedResource) {
    doDeleteResource(persistedResource);
  }

  private void doDeleteResource(final PersistedResource persistedResource) {
    final var resource =
        new ResourceRecord()
            .setResourceId(persistedResource.getResourceId())
            .setResourceKey(persistedResource.getResourceKey())
            .setTenantId(persistedResource.getTenantId())
            .setResourceName(persistedResource.getResourceName())
            .setResource(persistedResource.getResourceBuffer())
            .setChecksum(persistedResource.getChecksum())
            .setVersion(persistedResource.getVersion())
            .setVersionTag(persistedResource.getVersionTag())
            .setDeploymentKey(persistedResource.getDeploymentKey());
    stateWriter.appendFollowUpEvent(keyGenerator.nextKey(), ResourceIntent.DELETED, resource);
  }

  private void tryDeleteResources(
      final TypedRecord<ResourceRecord> command, final long resourceKey) {
    final var deleted =
        untilResourceDeleted(
            command, tenantId -> tryFindAndDeleteResource(command, resourceKey, tenantId));
    if (!deleted) {
      throw new NoSuchResourceException(resourceKey);
    }
  }

  private boolean untilResourceDeleted(
      final TypedRecord<ResourceRecord> command, final Function<String, Boolean> deletionCallback) {
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

  private boolean tryFindAndDeleteResource(
      final TypedRecord<ResourceRecord> command, final long resourceKey, final String tenantId) {
    final var resourceOptional = resourceState.findResourceByKey(resourceKey, tenantId);
    if (resourceOptional.isEmpty()) {
      return false;
    }
    final var resource = resourceOptional.get();
    checkAuthorization(command, resource);
    doDeleteResource(resource);
    return true;
  }

  private void checkAuthorization(
      final TypedRecord<ResourceRecord> command, final PersistedResource resource) {
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_RESOURCE)
            .tenantId(resource.getTenantId())
            .addResourceId(bufferAsString(resource.getResourceId()))
            .build();
    if (authCheckBehavior.isAuthorizedOrInternalCommand(authRequest).isLeft()) {
      throw new ForbiddenException(authRequest);
    }
  }
}
