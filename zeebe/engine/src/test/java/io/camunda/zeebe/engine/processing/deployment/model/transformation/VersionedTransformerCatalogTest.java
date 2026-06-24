/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.deployment.model.transformation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.camunda.zeebe.engine.processing.deployment.model.transformer.ErrorTransformer;
import org.junit.jupiter.api.Test;

class VersionedTransformerCatalogTest {

  @Test
  void shouldReturnEmptyForUnregisteredSlot() {
    // given
    final var catalog = VersionedTransformerCatalog.defaultCatalog();

    // when / then — nothing registered → caller uses in-code v1
    assertThat(catalog.resolve(TransformerSlot.ERROR, 2)).isEmpty();
    assertThat(catalog.currentVersion(TransformerSlot.ERROR))
        .isEqualTo(TransformerSlot.DEFAULT_VERSION);
  }

  @Test
  void shouldResolveExactVersionOnly() {
    // given — slot has v2 and v4 registered
    final var catalog =
        VersionedTransformerCatalog.builder()
            .register(TransformerSlot.ERROR, 2, ctx -> new ErrorTransformer())
            .register(TransformerSlot.ERROR, 4, ctx -> new ErrorTransformer())
            .build();

    // when / then — only exact matches resolve; floor/nearby versions do NOT silently substitute,
    // as that would produce a different pipeline than what the leader used at deploy time
    assertThat(catalog.resolve(TransformerSlot.ERROR, 2)).isPresent();
    assertThat(catalog.resolve(TransformerSlot.ERROR, 4)).isPresent();
    assertThat(catalog.resolve(TransformerSlot.ERROR, 3)).isEmpty(); // v3 not registered
    assertThat(catalog.currentVersion(TransformerSlot.ERROR)).isEqualTo(4);
  }

  @Test
  void shouldRejectRegisteringVersionOne() {
    // given
    final var builder = VersionedTransformerCatalog.builder();

    // when / then
    assertThatThrownBy(
            () -> builder.register(TransformerSlot.ERROR, 1, ctx -> new ErrorTransformer()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
