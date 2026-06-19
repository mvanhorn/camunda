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
import io.camunda.zeebe.engine.state.deployment.PersistedResource;
import io.camunda.zeebe.engine.state.immutable.ProcessingState;
import io.camunda.zeebe.engine.state.immutable.ResourceState;
import io.camunda.zeebe.engine.state.immutable.TenantState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ResourceRecord;
import io.camunda.zeebe.protocol.record.intent.ResourceIntent;
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
class ResourceDeleteProcessorTest {

  @Mock StateWriter stateWriter;
  @Mock TypedRejectionWriter rejectionWriter;
  @Mock TypedResponseWriter responseWriter;
  @Mock KeyGenerator keyGenerator;
  @Mock ResourceState resourceState;
  @Mock AuthorizationCheckBehavior authCheckBehavior;
  @Mock ProcessingState processingState;
  @Mock TenantState tenantState;
  @Mock Writers writers;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  CommandDistributionBehavior commandDistributionBehavior;

  ResourceDeleteProcessor processor;

  @BeforeEach
  void setUp() {
    when(writers.state()).thenReturn(stateWriter);
    when(writers.rejection()).thenReturn(rejectionWriter);
    when(writers.response()).thenReturn(responseWriter);
    when(processingState.getResourceState()).thenReturn(resourceState);
    when(processingState.getTenantState()).thenReturn(tenantState);
    processor =
        new ResourceDeleteProcessor(
            writers, keyGenerator, processingState, authCheckBehavior, commandDistributionBehavior);
  }

  @Test
  void shouldEmitDeletedEventForResource() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L);
    final var resource = buildPersistedResource(42L, "myResource", "tenant1");

    // when
    processor.deleteResource(resource);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ResourceIntent.DELETED), any());
  }

  @Test
  void shouldThrowWhenResourceNotFoundInProcessNewCommand() {
    // given
    final long resourceKey = 99L;
    when(resourceState.findResourceByKey(eq(resourceKey), any())).thenReturn(Optional.empty());
    final var command = mockResourceCommand(resourceKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ResourceDeletionExceptions.NoSuchResourceException.class);
  }

  @Test
  void shouldEmitDeletedEventsViaProcessNewCommand() {
    // given
    when(keyGenerator.nextKey()).thenReturn(100L);
    final long resourceKey = 55L;
    final var resource = buildPersistedResource(resourceKey, "myResource", "tenant1");
    when(resourceState.findResourceByKey(eq(resourceKey), eq("tenant1")))
        .thenReturn(Optional.of(resource));
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.right(null));
    final var command = mockResourceCommand(resourceKey, "tenant1");

    // when
    processor.processNewCommand(command);

    // then
    verify(stateWriter).appendFollowUpEvent(anyLong(), eq(ResourceIntent.DELETED), any());
  }

  @Test
  void shouldRejectWithForbiddenWhenNotAuthorized() {
    // given
    final long resourceKey = 55L;
    final var resource = buildPersistedResource(resourceKey, "myResource", "tenant1");
    when(resourceState.findResourceByKey(eq(resourceKey), eq("tenant1")))
        .thenReturn(Optional.of(resource));
    when(authCheckBehavior.isAuthorizedOrInternalCommand(any())).thenReturn(Either.left(null));
    final var command = mockResourceCommand(resourceKey, "tenant1");

    // when / then
    assertThatThrownBy(() -> processor.processNewCommand(command))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnExpectedErrorForForbiddenException() {
    // given
    final var command = (TypedRecord<ResourceRecord>) mock(TypedRecord.class);
    final var authRequest =
        AuthorizationRequest.builder()
            .command(command)
            .resourceType(AuthorizationResourceType.RESOURCE)
            .permissionType(PermissionType.DELETE_RESOURCE)
            .tenantId("tenant1")
            .addResourceId("myResource")
            .build();
    final var exception = new ForbiddenException(authRequest);

    // when
    final var result = processor.tryHandleError(command, exception);

    // then
    assertThat(result).isEqualTo(ProcessingError.EXPECTED_ERROR);
  }

  private PersistedResource buildPersistedResource(
      final long resourceKey, final String resourceId, final String tenantId) {
    final var resourceRecord =
        new ResourceRecord()
            .setResourceKey(resourceKey)
            .setResourceId(resourceId)
            .setVersion(1)
            .setResourceName("resource.name")
            .setTenantId(tenantId)
            .setDeploymentKey(1L)
            .setVersionTag("");
    return new PersistedResource().wrap(resourceRecord);
  }

  @SuppressWarnings("unchecked")
  private TypedRecord<ResourceRecord> mockResourceCommand(
      final long resourceKey, final String tenantId) {
    final var command = (TypedRecord<ResourceRecord>) mock(TypedRecord.class);
    final var record = new ResourceRecord().setResourceKey(resourceKey).setTenantId(tenantId);
    when(command.getValue()).thenReturn(record);
    return command;
  }
}
