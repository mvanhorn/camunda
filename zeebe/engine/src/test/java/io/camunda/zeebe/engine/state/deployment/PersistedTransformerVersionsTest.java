/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.deployment;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.engine.processing.deployment.model.transformation.TransformerSlot;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

class PersistedTransformerVersionsTest {

  @Test
  void shouldRoundTripSparseVersions() {
    // given
    final var original = new PersistedTransformerVersions();
    original.setVersions(Map.of(TransformerSlot.ERROR.id(), 2, TransformerSlot.END_EVENT.id(), 3));

    // when — serialize then deserialize
    final var buffer = new UnsafeBuffer(new byte[original.getLength()]);
    original.write(buffer, 0);
    final var copy = new PersistedTransformerVersions();
    copy.wrap(buffer, 0, buffer.capacity());

    // then
    assertThat(copy.asMap())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(TransformerSlot.ERROR.id(), 2, TransformerSlot.END_EVENT.id(), 3));
  }

  @Test
  void shouldSkipDefaultVersionEntries() {
    // given
    final var versions = new PersistedTransformerVersions();

    // when — version 1 is the default and must not be stored
    versions.setVersions(Map.of(TransformerSlot.ERROR.id(), 1, TransformerSlot.SIGNAL.id(), 2));

    // then
    assertThat(versions.asMap()).containsExactlyEntriesOf(Map.of(TransformerSlot.SIGNAL.id(), 2));
  }

  @Test
  void shouldBeEmptyWhenAllDefault() {
    // given
    final var versions = new PersistedTransformerVersions();

    // when
    versions.setVersions(Map.of(TransformerSlot.ERROR.id(), 1));

    // then
    assertThat(versions.isEmpty()).isTrue();
  }
}
