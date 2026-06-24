/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.deployment;

import io.camunda.zeebe.msgpack.UnpackedObject;
import io.camunda.zeebe.msgpack.property.IntegerProperty;

public final class PersistedTransformerVersionEntry extends UnpackedObject {

  private final IntegerProperty slotIdProp = new IntegerProperty("slotId");
  private final IntegerProperty versionProp = new IntegerProperty("version");

  public PersistedTransformerVersionEntry() {
    super(2);
    declareProperty(slotIdProp).declareProperty(versionProp);
  }

  public int getSlotId() {
    return slotIdProp.getValue();
  }

  public PersistedTransformerVersionEntry setSlotId(final int slotId) {
    slotIdProp.setValue(slotId);
    return this;
  }

  public int getVersion() {
    return versionProp.getValue();
  }

  public PersistedTransformerVersionEntry setVersion(final int version) {
    versionProp.setValue(version);
    return this;
  }
}
