/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.deployment.model.transformation;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Registry of versioned BPMN sub-transformers. Version 1 of every transformer lives directly in
 * {@link BpmnTransformer}'s constructor as the original class; this catalog holds ONLY versions
 * greater than 1. It is empty until a sub-transformer needs to change behavior.
 *
 * <p>To version a transformer: write {@code XTransformerV2} (leaving the original frozen) and
 * register it here at version 2 via {@link Builder#register}.
 */
public final class VersionedTransformerCatalog {

  private final Map<TransformerSlot, TreeMap<Integer, Factory>> bySlot;

  private VersionedTransformerCatalog(
      final Map<TransformerSlot, TreeMap<Integer, Factory>> bySlot) {
    this.bySlot = bySlot;
  }

  /** The production catalog. Empty today — populated as sub-transformers gain new versions. */
  public static VersionedTransformerCatalog defaultCatalog() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the factory registered for {@code slot} at exactly {@code requestedVersion}, or empty
   * if no factory is registered at that version. Callers must treat empty as a fatal
   * misconfiguration when {@code requestedVersion > DEFAULT_VERSION}: the process was deployed with
   * that exact version active, and using any other version would diverge from the leader's
   * pipeline.
   */
  public Optional<Factory> resolve(final TransformerSlot slot, final int requestedVersion) {
    final var versions = bySlot.get(slot);
    if (versions == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(versions.get(requestedVersion));
  }

  /**
   * Highest registered version for the slot, or {@link TransformerSlot#DEFAULT_VERSION} if none.
   */
  public int currentVersion(final TransformerSlot slot) {
    final var versions = bySlot.get(slot);
    return versions == null ? TransformerSlot.DEFAULT_VERSION : versions.lastKey();
  }

  @FunctionalInterface
  public interface Factory
      extends Function<TransformerFactoryContext, ModelElementTransformer<?>> {}

  public static final class Builder {
    private final Map<TransformerSlot, TreeMap<Integer, Factory>> bySlot =
        new EnumMap<>(TransformerSlot.class);

    public Builder register(final TransformerSlot slot, final int version, final Factory factory) {
      if (version < 2) {
        throw new IllegalArgumentException(
            "Version 1 lives in code, not the catalog; got version " + version + " for " + slot);
      }
      bySlot.computeIfAbsent(slot, s -> new TreeMap<>()).put(version, factory);
      return this;
    }

    public VersionedTransformerCatalog build() {
      final Map<TransformerSlot, TreeMap<Integer, Factory>> copy =
          new EnumMap<>(TransformerSlot.class);
      bySlot.forEach((slot, versions) -> copy.put(slot, new TreeMap<>(versions)));
      return new VersionedTransformerCatalog(copy);
    }
  }
}
