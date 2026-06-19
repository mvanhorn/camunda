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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.engine.processing.distribution.CommandDistributionBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.AuthorizationCheckBehavior;
import io.camunda.zeebe.engine.processing.identity.authorization.exception.ForbiddenException;
import io.camunda.zeebe.engine.processing.identity.authorization.request.AuthorizationRequest;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor.ProcessingError;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.state.deployment.PersistedForm;
import io.camunda.zeebe.engine.state.immutable.FormState;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.FormRecord;
import io.camunda.zeebe.protocol.record.intent.FormIntent;
import io.camunda.zeebe.protocol.record.value.AuthorizationResourceType;
import io.camunda.zeebe.protocol.record.value.PermissionType;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import io.camunda.zeebe.stream.api.state.KeyGenerator;
import io.camunda.zeebe.util.Either;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FormDeleteProcessorTest {

  @Mock StateWriter stateWriter;
  @Mock TypedRejectionWriter rejectionWriter;
  @Mock TypedResponseWriter responseWriter;
  @Mock KeyGenerator keyGenerator;
  @Mock FormState formState;
  @Mock AuthorizationCheckBehavior authCheckBehavior;
  @Mock ProcessingState processingState;
  @Mock TenantState tenantState;
  @Mock Writers writers;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  CommandDistributionBehavior commandDistributionBehavior;

  FormDeleteProcessor processor;

  @BeforeEach
  void setUp() {
    when(writers.state()).thenReturn(stateWriter);
    when(writers.rejection()).thenReturn(rejectionWriter);
    when(writers.response()).thenReturn(responseWriter);
    when(processingState.getFormState()).thenReturn(formState);
    when(processingState.getTenantState()).thenReturn(tenantState);
    processor =
        new FormDeleteProcessor(
            writers, keyGenerator, processingState, authCheckBehavior, commandDistributionBehavior);
  }

  @Test
  void shouldEmitDeletedEventForForm() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L);
    final var form = buildPersistedForm(42L, "myForm", "tenant1");

    // when
    processor.deleteForm(form);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(FormIntent.DELETED), any());
  }

  @Test
  void shouldThrowWhenFormNotFoundInProcessNewCommand() {
    // given
    final long formKey = 99L;
    when(formState.findFormByKey(eq(formKey), any())).thenReturn(Optional.empty());
    final var command = mockFormCommand(formKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ResourceDeletionExceptions.NoSuchResourceException.class);
  }

  @Test
  void shouldEmitDeletedEventsViaProcessNewCommand() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L);
    final long formKey = 55L;
    final var form = buildPersistedForm(formKey, "myForm", "tenant1");
    when(formState.findFormByKey(eq(formKey), eq("tenant1"))).thenReturn(Optional.of(form));
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.right(null));
    final var command = mockFormCommand(formKey, "tenant1");

    // when
    processor.processNewCommand(command);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(FormIntent.DELETED), any());
  }

  @Test
  void shouldRejectWithForbiddenWhenNotAuthorized() {
    // given
    final long formKey = 55L;
    final var form = buildPersistedForm(formKey, "myForm", "tenant1");
    when(formState.findFormByKey(eq(formKey), eq("tenant1"))).thenReturn(Optional.of(form));
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.left(null));
    final var command = mockFormCommand(formKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnExpectedErrorForForbiddenException() {
    // given
    final var command = (TypedRecord<FormRecord>) mock(TypedRecord.class);
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_FORM)
            .tenantId("tenant1")
            .addResourceId("myForm")
            .build();
    final var exception = new ForbiddenException(authRequest);

    // when
    final var result = processor.tryHandleError(command, exception);

    // then
    assertThat(result).isEqualTo(ProcessingError.EXPECTED_ERROR);
  }

  // --- helpers ---

  private PersistedForm buildPersistedForm(
      final long formKey, final String formId, final String tenantId) {
    final var formRecord =
        new FormRecord()
            .setFormKey(formKey)
            .setFormId(formId)
            .setVersion(1)
            .setResourceName("form.form")
            .setTenantId(tenantId)
            .setDeploymentKey(1L)
            .setVersionTag("");
    return new PersistedForm().wrap(formRecord);
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<FormRecord> mockFormCommand(final long formKey, final String tenantId) {
    final var command = (TypedRecord<FormRecord>) mock(TypedRecord.class);
    final var record = new FormRecord().setFormKey(formKey).setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    return command;
  }
}
