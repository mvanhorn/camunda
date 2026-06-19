/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.application.commons.search;

import io.camunda.configuration.SecondaryStorage.SecondaryStorageType;
import io.camunda.configuration.beans.SearchEngineConnectProperties;
import io.camunda.configuration.beans.SearchEngineIndexProperties;
import io.camunda.configuration.beans.SearchEngineRetentionProperties;
import io.camunda.configuration.beans.SearchEngineSchemaManagerProperties;
import io.camunda.configuration.conditions.ConditionalOnSecondaryStorageType;
import io.camunda.operate.property.OperateProperties;
import io.camunda.search.connect.configuration.DatabaseConfig;
import io.camunda.search.connect.configuration.DatabaseType;
import io.camunda.search.schema.config.SearchEngineConfiguration;
import io.camunda.tasklist.property.TasklistProperties;
import io.camunda.zeebe.broker.Broker;
import io.camunda.zeebe.broker.system.configuration.BrokerCfg;
import io.micrometer.core.instrument.MeterRegistry;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnSecondaryStorageType({
  SecondaryStorageType.elasticsearch,
  SecondaryStorageType.opensearch
})
public class SearchEngineDatabaseConfiguration {

  @Bean
  public SearchEngineSchemaInitializer searchEngineSchemaInitializer(
      final SearchEngineConfiguration searchEngineConfiguration,
      final MeterRegistry meterRegistry,
      @Autowired(required = false)
          final Broker broker, // if present, then it will ensure that the broker is started first
      @Autowired(required = false) final BrokerCfg brokerCfg,
      @Autowired(required = false) final OperateProperties operateProperties,
      @Autowired(required = false) final TasklistProperties tasklistProperties) {
    final boolean isGatewayEnabled = brokerCfg == null || brokerCfg.getGateway().isEnable();
    final boolean healthCheckEnabled =
        resolveHealthCheckEnabled(searchEngineConfiguration, operateProperties, tasklistProperties);
    return new SearchEngineSchemaInitializer(
        searchEngineConfiguration, meterRegistry, isGatewayEnabled, healthCheckEnabled);
  }

  private static boolean resolveHealthCheckEnabled(
      final SearchEngineConfiguration config,
      final @Nullable OperateProperties operateProperties,
      final @Nullable TasklistProperties tasklistProperties) {
    // Unified property takes precedence if explicitly set
    final Boolean unified = config.schemaManager().getHealthCheckEnabled();
    if (unified != null) {
      return unified;
    }
    // Fall back to legacy per-module properties; AND semantics: any false disables the check
    final boolean isEs = config.connect().getTypeEnum().isElasticSearch();
    boolean effective = true;
    if (operateProperties != null) {
      effective &=
          isEs
              ? operateProperties.getElasticsearch().isHealthCheckEnabled()
              : operateProperties.getOpensearch().isHealthCheckEnabled();
    }
    if (tasklistProperties != null) {
      effective &=
          isEs
              ? tasklistProperties.getElasticsearch().isHealthCheckEnabled()
              : tasklistProperties.getOpenSearch().isHealthCheckEnabled();
    }
    return effective;
  }

  @Bean
  public SearchEngineConfiguration searchEngineConfiguration(
      final SearchEngineConnectProperties searchEngineConnectProperties,
      final SearchEngineIndexProperties searchEngineIndexProperties,
      final SearchEngineRetentionProperties searchEngineRetentionProperties,
      final SearchEngineSchemaManagerProperties searchEngineSchemaManagerProperties) {

    // Override schema creation if database type is "none"
    final DatabaseType databaseType = searchEngineConnectProperties.getTypeEnum();
    if (DatabaseConfig.NONE.equalsIgnoreCase(databaseType.name())) {
      searchEngineSchemaManagerProperties.setCreateSchema(false);
    }

    return SearchEngineConfiguration.of(
        b ->
            b.connect(searchEngineConnectProperties)
                .index(searchEngineIndexProperties)
                .retention(searchEngineRetentionProperties)
                .schemaManager(searchEngineSchemaManagerProperties));
  }
}
