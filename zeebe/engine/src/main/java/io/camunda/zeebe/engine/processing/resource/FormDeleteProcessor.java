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
import io.camunda.zeebe.engine.state.deployment.PersistedForm;
import io.camunda.zeebe.engine.state.distribution.DistributionQueue;
import io.camunda.zeebe.engine.state.immutable.FormState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.FormRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.FormIntent;
import io.camunda.zeebe.protocol.record.value.AuthorizationResourceType;
import io.camunda.zeebe.protocol.record.value.PermissionType;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.VisibleForTesting;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class FormDeleteProcessor implements DistributedTypedRecordProcessor<FormRecord> {

  private final StateWriter stateWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final TypedResponseWriter responseWriter;
  private final KeyGenerator keyGenerator;
  private final FormState formState;
  private final AuthorizationCheckBehavior authCheckBehavior;
  private final CommandDistributionBehavior commandDistributionBehavior;
  private final TenantState tenantState;

  public FormDeleteProcessor(
      final Writers writers,
      final KeyGenerator keyGenerator,
      final ProcessingState processingState,
      final AuthorizationCheckBehavior authCheckBehavior,
      final CommandDistributionBehavior commandDistributionBehavior) {
    stateWriter = writers.state();
    rejectionWriter = writers.rejection();
    responseWriter = writers.response();
    this.keyGenerator = keyGenerator;
    formState = processingState.getFormState();
    this.authCheckBehavior = authCheckBehavior;
    this.commandDistributionBehavior = commandDistributionBehavior;
    tenantState = processingState.getTenantState();
  }

  @Override
  public void processNewCommand(final TypedRecord<FormRecord> command) {
    final long formKey = command.getValue().getFormKey();
    final long eventKey = keyGenerator.nextKey();

    tryDeleteForms(command, formKey);

    commandDistributionBehavior
        .withKey(eventKey)
        .inQueue(DistributionQueue.DEPLOYMENT)
        .distribute(command);
    responseWriter.writeEventOnCommand(eventKey, FormIntent.DELETED, command.getValue(), command);
  }

  @Override
  public void processDistributedCommand(final TypedRecord<FormRecord> command) {
    final long formKey = command.getValue().getFormKey();

    tryDeleteForms(command, formKey);

    commandDistributionBehavior.acknowledgeCommand(command);
  }

  @Override
  public ProcessingError tryHandleError(
      final TypedRecord<FormRecord> command, final Throwable error) {
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
  void deleteForm(final PersistedForm persistedForm) {
    doDeleteForm(persistedForm);
  }

  private void doDeleteForm(final PersistedForm persistedForm) {
    final var form =
        new FormRecord()
            .setFormId(persistedForm.getFormId())
            .setFormKey(persistedForm.getFormKey())
            .setTenantId(persistedForm.getTenantId())
            .setResourceName(persistedForm.getResourceName())
            .setResource(persistedForm.getResource())
            .setChecksum(persistedForm.getChecksum())
            .setVersion(persistedForm.getVersion())
            .setVersionTag(persistedForm.getVersionTag())
            .setDeploymentKey(persistedForm.getDeploymentKey());
    stateWriter.appendFollowUpEvent(keyGenerator.nextKey(), FormIntent.DELETED, form);
  }

  private void tryDeleteForms(final TypedRecord<FormRecord> command, final long formKey) {
    final var deleted =
        untilFormDeleted(command, tenantId -> tryFindAndDeleteForm(command, formKey, tenantId));
    if (!deleted) {
      throw new NoSuchResourceException(formKey);
    }
  }

  private boolean untilFormDeleted(
      final TypedRecord<FormRecord> command, final Function<String, Boolean> deletionCallback) {
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

  private boolean tryFindAndDeleteForm(
      final TypedRecord<FormRecord> command, final long formKey, final String tenantId) {
    final var formOptional = formState.findFormByKey(formKey, tenantId);
    if (formOptional.isEmpty()) {
      return false;
    }
    final var form = formOptional.get();
    checkAuthorization(command, form);
    doDeleteForm(form);
    return true;
  }

  private void checkAuthorization(final TypedRecord<FormRecord> command, final PersistedForm form) {
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_FORM)
            .tenantId(form.getTenantId())
            .addResourceId(bufferAsString(form.getFormId()))
            .build();
    if (authCheckBehavior.isAuthorizedOrInternalCommand(authRequest).isLeft()) {
      throw new ForbiddenException(authRequest);
    }
  }
}
