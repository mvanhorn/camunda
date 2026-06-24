/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.deployment;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.ArrayProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Sparse per-process map of {@code slotId -> transformer version}. Only versions > 1 are stored.
 */
public final class PersistedTransformerVersions extends UnpackedObject implements DbValue {

  private final ArrayProperty<PersistedTransformerVersionEntry> entriesProp =
      new ArrayProperty<>("entries", PersistedTransformerVersionEntry::new);

  public PersistedTransformerVersions() {
    super(1);
    declareProperty(entriesProp);
  }

  /** Replaces all entries with the given sparse map (entries with version <= 1 are skipped). */
  public PersistedTransformerVersions setVersions(final Map<Integer, Integer> versions) {
    entriesProp.reset();
    new TreeMap<>(versions)
        .forEach(
            (slotId, version) -> {
              if (version != null && version > 1) {
                entriesProp.add().setSlotId(slotId).setVersion(version);
              }
            });
    return this;
  }

  public Map<Integer, Integer> asMap() {
    final Map<Integer, Integer> result = new HashMap<>();
    entriesProp.forEach(e -> result.put(e.getSlotId(), e.getVersion()));
    return result;
  }

  public boolean isEmpty() {
    return !entriesProp.iterator().hasNext();
  }
}
