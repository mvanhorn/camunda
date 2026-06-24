/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.state.deployment;

import static io.camunda.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.camunda.zeebe.engine.processing.deployment.model.transformation.TransformerSlot;
import io.camunda.zeebe.engine.state.mutable.MutableProcessingState;
import io.camunda.zeebe.engine.util.ProcessingStateExtension;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.camunda.zeebe.protocol.record.value.TenantOwned;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ProcessingStateExtension.class)
class DbProcessStateTransformerVersionTest {

  private MutableProcessingState processingState;

  @Test
  void shouldResolveFlowElementForProcessWithoutStoredVersions() {
    // given — legacy process: no transformer-version row
    final var model = Bpmn.createExecutableProcess("legacy").startEvent("s").endEvent("e").done();
    final var record = new ProcessRecord();
    record
        .setResourceName("legacy.bpmn")
        .setResource(wrapString(Bpmn.convertToString(model)))
        .setTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER)
        .setBpmnProcessId(wrapString("legacy"))
        .setVersion(1)
        .setChecksum(wrapString("checksum"));
    final var processState = processingState.getProcessState();
    processState.putProcess(7L, record);

    // when
    final var element =
        processState.getFlowElement(
            7L,
            TenantOwned.DEFAULT_TENANT_IDENTIFIER,
            wrapString("s"),
            ExecutableFlowElement.class);

    // then — default (v1) pipeline resolves the element
    assertThat(element).isNotNull();
  }

  @Test
  void shouldSkipDefaultVersionsWhenStoringAndStillResolveProcess() {
    // given — a process whose version map contains only default (v1) entries
    final var model8 = Bpmn.createExecutableProcess("p8").startEvent("s8").endEvent("e8").done();
    final var processState = processingState.getProcessState();
    final var record8 = new ProcessRecord();
    record8
        .setResourceName("p8.bpmn")
        .setResource(wrapString(Bpmn.convertToString(model8)))
        .setTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER)
        .setBpmnProcessId(wrapString("p8"))
        .setVersion(1)
        .setChecksum(wrapString("checksum8"));
    processState.putProcess(8L, record8);

    // when — all-v1 map: storeTransformerVersions must skip all entries (writes no row)
    processState.storeTransformerVersions(
        8L, TenantOwned.DEFAULT_TENANT_IDENTIFIER, Map.of(TransformerSlot.ERROR, 1));

    // then — no row → absent = v1 contract → default (all-v1) transformer resolves the element
    processState.clearCache();
    assertThat(
            processState.getFlowElement(
                8L,
                TenantOwned.DEFAULT_TENANT_IDENTIFIER,
                wrapString("s8"),
                ExecutableFlowElement.class))
        .isNotNull();
  }

  @Test
  void shouldThrowWhenPinnedVersionMissingFromCatalog() {
    // given — a non-default slot version stored in the DB but no matching factory in the catalog
    final var model9 = Bpmn.createExecutableProcess("p9").startEvent("s9").endEvent("e9").done();
    final var processState = processingState.getProcessState();
    final var record9 = new ProcessRecord();
    record9
        .setResourceName("p9.bpmn")
        .setResource(wrapString(Bpmn.convertToString(model9)))
        .setTenantId(TenantOwned.DEFAULT_TENANT_IDENTIFIER)
        .setBpmnProcessId(wrapString("p9"))
        .setVersion(1)
        .setChecksum(wrapString("checksum9"));
    processState.putProcess(9L, record9);
    processState.storeTransformerVersions(
        9L, TenantOwned.DEFAULT_TENANT_IDENTIFIER, Map.of(TransformerSlot.END_EVENT, 2));

    // when / then — getFlowElement triggers cache-miss → reads stored v2 for END_EVENT → no
    // factory registered → must fail fast rather than silently fall back to v1
    processState.clearCache();
    assertThatThrownBy(
            () ->
                processState.getFlowElement(
                    9L,
                    TenantOwned.DEFAULT_TENANT_IDENTIFIER,
                    wrapString("s9"),
                    ExecutableFlowElement.class))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("END_EVENT")
        .hasMessageContaining("version 2");
  }
}
