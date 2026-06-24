/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.appliers;

import io.camunda.zeebe.engine.processing.deployment.model.transformation.TransformerSlot;
import io.camunda.zeebe.engine.state.TypedEventApplier;
import io.camunda.zeebe.engine.state.mutable.MutableProcessState;
import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.camunda.zeebe.protocol.record.intent.ProcessIntent;
import java.util.Map;

final class ProcessCreatedV3Applier implements TypedEventApplier<ProcessIntent, ProcessRecord> {

  /**
   * The per-slot transformer versions current at the time THIS applier version was written. Frozen
   * forever — never edit. When a sub-transformer is first bumped to v2, add a new
   * ProcessCreatedV4Applier whose constant includes that slot, leaving this one untouched.
   *
   * <p>Today every sub-transformer is at version 1, so this map is empty (and, being sparse, {@code
   * storeTransformerVersions} writes no row).
   */
  static final Map<TransformerSlot, Integer> TRANSFORMER_VERSIONS = Map.of();

  private final MutableProcessState processState;

  ProcessCreatedV3Applier(final MutableProcessingState state) {
    processState = state.getProcessState();
  }

  @Override
  public void applyState(final long processDefinitionKey, final ProcessRecord value) {
    processState.putProcess(processDefinitionKey, value);
    processState.storeProcessDefinitionKeyByProcessIdAndDeploymentKey(value);
    processState.storeProcessDefinitionKeyByProcessIdAndVersionTag(value);
    processState.storeTransformerVersions(
        processDefinitionKey, value.getTenantId(), TRANSFORMER_VERSIONS);
  }
}
