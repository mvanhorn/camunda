/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.deployment.model.transformation;

import io.camunda.zeebe.el.ExpressionLanguage;
import io.camunda.zeebe.engine.EngineConfiguration;
import io.camunda.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.AdHocSubProcessTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.BoundaryEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.BusinessRuleTaskTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.CallActivityTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.CatchEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ConditionalTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ContextProcessTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.EndEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ErrorTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.EscalationTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.EventBasedGatewayTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ExclusiveGatewayTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.FlowElementInstantiationTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.FlowNodeTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.InclusiveGatewayTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.IntermediateCatchEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.IntermediateThrowEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.JobWorkerElementTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.MessageTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.MultiInstanceActivityTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ProcessTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ReceiveTaskTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.ScriptTaskTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.SequenceFlowTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.SignalTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.StartEventTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.SubProcessTransformer;
import io.camunda.zeebe.engine.processing.deployment.model.transformer.UserTaskTransformer;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.BpmnModelElementInstance;
import io.camunda.zeebe.model.bpmn.instance.SendTask;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.traversal.ModelWalker;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class BpmnTransformer {

  /*
   * Step 1: Instantiate all elements in the process
   */
  private final TransformationVisitor step1Visitor;

  /*
   * Step 2: Transform all attributes, cross-link elements, etc.
   */
  private final TransformationVisitor step2Visitor;

  /*
   * Step 3: Modify elements based on the context
   */
  private final TransformationVisitor step3Visitor;

  /*
   * Step 4: Modify elements based on containing elements
   */
  private final TransformationVisitor step4Visitor;

  /*
   * Step 5: Modify elements based on containing container elements
   */
  private final TransformationVisitor step5Visitor;

  private final ExpressionLanguage expressionLanguage;
  private final int maxNameFieldLength;
  private final VersionedTransformerCatalog catalog;
  private final Map<TransformerSlot, Integer> slotVersions;
  private final TransformerFactoryContext factoryContext;

  public BpmnTransformer(final ExpressionLanguage expressionLanguage) {
    this(expressionLanguage, EngineConfiguration.DEFAULT_MAX_NAME_FIELD_LENGTH);
  }

  public BpmnTransformer(
      final ExpressionLanguage expressionLanguage, final int maxNameFieldLength) {
    this(
        expressionLanguage,
        maxNameFieldLength,
        VersionedTransformerCatalog.defaultCatalog(),
        Map.of());
  }

  public BpmnTransformer(
      final ExpressionLanguage expressionLanguage,
      final int maxNameFieldLength,
      final VersionedTransformerCatalog catalog,
      final Map<TransformerSlot, Integer> slotVersions) {
    this.expressionLanguage = expressionLanguage;
    this.maxNameFieldLength = maxNameFieldLength;
    this.catalog = catalog;
    this.slotVersions = slotVersions;
    factoryContext = new TransformerFactoryContext(expressionLanguage, maxNameFieldLength);

    step1Visitor = new TransformationVisitor();
    step1Visitor.registerHandler(handlerFor(TransformerSlot.ERROR, ErrorTransformer::new));
    step1Visitor.registerHandler(
        handlerFor(TransformerSlot.ESCALATION, EscalationTransformer::new));
    step1Visitor.registerHandler(
        handlerFor(
            TransformerSlot.FLOW_ELEMENT_INSTANTIATION, FlowElementInstantiationTransformer::new));
    step1Visitor.registerHandler(handlerFor(TransformerSlot.MESSAGE, MessageTransformer::new));
    step1Visitor.registerHandler(handlerFor(TransformerSlot.SIGNAL, SignalTransformer::new));
    step1Visitor.registerHandler(
        handlerFor(TransformerSlot.CONDITIONAL, ConditionalTransformer::new));
    step1Visitor.registerHandler(handlerFor(TransformerSlot.PROCESS, ProcessTransformer::new));

    step2Visitor = new TransformationVisitor();
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.BOUNDARY_EVENT, BoundaryEventTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.BUSINESS_RULE_TASK, BusinessRuleTaskTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.CALL_ACTIVITY, CallActivityTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.CATCH_EVENT, CatchEventTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.CONTEXT_PROCESS, ContextProcessTransformer::new));
    step2Visitor.registerHandler(handlerFor(TransformerSlot.END_EVENT, EndEventTransformer::new));
    step2Visitor.registerHandler(handlerFor(TransformerSlot.FLOW_NODE, FlowNodeTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(
            TransformerSlot.SERVICE_TASK_JOB_WORKER,
            () -> new JobWorkerElementTransformer<>(ServiceTask.class)));
    step2Visitor.registerHandler(
        handlerFor(
            TransformerSlot.SEND_TASK_JOB_WORKER,
            () -> new JobWorkerElementTransformer<>(SendTask.class)));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.RECEIVE_TASK, ReceiveTaskTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.SCRIPT_TASK, ScriptTaskTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.SEQUENCE_FLOW, SequenceFlowTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.START_EVENT, StartEventTransformer::new));
    step2Visitor.registerHandler(
        handlerFor(TransformerSlot.USER_TASK, () -> new UserTaskTransformer(expressionLanguage)));

    step3Visitor = new TransformationVisitor();
    step3Visitor.registerHandler(
        handlerFor(TransformerSlot.CONTEXT_PROCESS, ContextProcessTransformer::new));
    step3Visitor.registerHandler(
        handlerFor(TransformerSlot.EVENT_BASED_GATEWAY, EventBasedGatewayTransformer::new));
    step3Visitor.registerHandler(
        handlerFor(TransformerSlot.EXCLUSIVE_GATEWAY, ExclusiveGatewayTransformer::new));
    step3Visitor.registerHandler(
        handlerFor(TransformerSlot.INCLUSIVE_GATEWAY, InclusiveGatewayTransformer::new));
    step3Visitor.registerHandler(
        handlerFor(
            TransformerSlot.INTERMEDIATE_CATCH_EVENT, IntermediateCatchEventTransformer::new));
    step3Visitor.registerHandler(
        handlerFor(TransformerSlot.SUB_PROCESS, SubProcessTransformer::new));

    step4Visitor = new TransformationVisitor();
    step4Visitor.registerHandler(
        handlerFor(TransformerSlot.CONTEXT_PROCESS, ContextProcessTransformer::new));
    step4Visitor.registerHandler(
        handlerFor(
            TransformerSlot.INTERMEDIATE_THROW_EVENT, IntermediateThrowEventTransformer::new));
    step4Visitor.registerHandler(
        handlerFor(TransformerSlot.AD_HOC_SUB_PROCESS, AdHocSubProcessTransformer::new));

    step5Visitor = new TransformationVisitor();
    step5Visitor.registerHandler(
        handlerFor(TransformerSlot.CONTEXT_PROCESS, ContextProcessTransformer::new));
    step5Visitor.registerHandler(
        handlerFor(TransformerSlot.MULTI_INSTANCE_ACTIVITY, MultiInstanceActivityTransformer::new));
  }

  public List<ExecutableProcess> transformDefinitions(final BpmnModelInstance modelInstance) {
    final TransformContext context = new TransformContext();
    context.setExpressionLanguage(expressionLanguage);
    context.setMaxNameFieldLength(maxNameFieldLength);

    final ModelWalker walker = new ModelWalker(modelInstance);
    step1Visitor.setContext(context);
    walker.walk(step1Visitor);

    step2Visitor.setContext(context);
    walker.walk(step2Visitor);

    step3Visitor.setContext(context);
    walker.walk(step3Visitor);

    step4Visitor.setContext(context);
    walker.walk(step4Visitor);

    step5Visitor.setContext(context);
    walker.walk(step5Visitor);

    return context.getProcesses();
  }

  @SuppressWarnings("unchecked")
  private <T extends BpmnModelElementInstance> ModelElementTransformer<T> handlerFor(
      final TransformerSlot slot, final Supplier<ModelElementTransformer<T>> v1Factory) {
    final int requested = slotVersions.getOrDefault(slot, TransformerSlot.DEFAULT_VERSION);
    if (requested <= TransformerSlot.DEFAULT_VERSION) {
      return v1Factory.get();
    }
    return catalog
        .resolve(slot, requested)
        .map(f -> (ModelElementTransformer<T>) f.apply(factoryContext))
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Slot "
                        + slot
                        + " was pinned to version "
                        + requested
                        + " at deploy time but no factory is registered at exactly that version."
                        + " The catalog is missing a handler that existed when the process was deployed."));
  }
}
