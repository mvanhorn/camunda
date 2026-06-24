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

import io.camunda.zeebe.el.ExpressionLanguage;
import io.camunda.zeebe.el.ExpressionLanguageFactory;
import io.camunda.zeebe.el.ExpressionLanguageMetrics;
import io.camunda.zeebe.engine.processing.bpmn.clock.ZeebeFeelEngineClock;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.SignalTransformer;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.instance.Signal;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class BpmnTransformerVersioningTest {

  private static ExpressionLanguage expressionLanguage() {
    return ExpressionLanguageFactory.createExpressionLanguage(
        new ZeebeFeelEngineClock(InstantSource.system()), ExpressionLanguageMetrics.noop());
  }

  @Test
  void shouldTransformIdenticallyWithEmptyVersions() {
    // given
    final var model =
        Bpmn.createExecutableProcess("p")
            .startEvent("s")
            .serviceTask("t", t -> t.zeebeJobType("w"))
            .endEvent("e")
            .done();
    final var transformer =
        new BpmnTransformer(
            expressionLanguage(),
            Integer.MAX_VALUE,
            VersionedTransformerCatalog.defaultCatalog(),
            Map.of());

    // when
    final List<ExecutableProcess> result = transformer.transformDefinitions(model);

    // then — the default (v1) pipeline produces the expected element types
    assertThat(result).hasSize(1);
    final ExecutableFlowElement task =
        result.get(0).getElementById("t", ExecutableFlowElement.class);
    assertThat(task.getElementType()).isEqualTo(BpmnElementType.SERVICE_TASK);
  }

  @Test
  void shouldUseCatalogV2WhenSlotVersionRequested() {
    // given — a delegating v2 SIGNAL handler that records it ran
    final var ran = new AtomicBoolean(false);
    final VersionedTransformerCatalog catalog =
        VersionedTransformerCatalog.builder()
            .register(
                TransformerSlot.SIGNAL,
                2,
                ctx ->
                    new ModelElementTransformer<Signal>() {
                      private final SignalTransformer delegate = new SignalTransformer();

                      @Override
                      public Class<Signal> getType() {
                        return Signal.class;
                      }

                      @Override
                      public void transform(final Signal element, final TransformContext context) {
                        ran.set(true);
                        delegate.transform(element, context);
                      }
                    })
            .build();
    final var model =
        Bpmn.createExecutableProcess("p")
            .startEvent()
            .intermediateCatchEvent("catch")
            .signal("sig")
            .endEvent()
            .done();
    final var transformer =
        new BpmnTransformer(
            expressionLanguage(), Integer.MAX_VALUE, catalog, Map.of(TransformerSlot.SIGNAL, 2));

    // when
    transformer.transformDefinitions(model);

    // then — the v2 handler was used for the SIGNAL slot
    assertThat(ran).isTrue();
  }

  @Test
  void shouldThrowWhenPinnedVersionHasNoRegisteredFactory() {
    // given / when / then — construction fails immediately when a pinned version has no factory,
    // rather than silently falling back to v1 which would diverge from the leader's state
    assertThatThrownBy(
            () ->
                new BpmnTransformer(
                    expressionLanguage(),
                    Integer.MAX_VALUE,
                    VersionedTransformerCatalog.defaultCatalog(),
                    Map.of(TransformerSlot.SIGNAL, 2)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SIGNAL")
        .hasMessageContaining("version 2");
  }
}
