/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.webapps.schema.entities.clustervariable;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.webapps.schema.descriptors.index.ClusterVariableIndex;
import io.camunda.webapps.schema.entities.clustervariable.ClusterVariableEntity.MetadataEntry;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterVariableEntityTest {

  @Test
  void shouldExposeMetadataIndexConstant() {
    // then
    assertThat(ClusterVariableIndex.METADATA).isEqualTo("metadata");
  }

  @Test
  void shouldConstructMetadataEntryWithAllFields() {
    // when
    final MetadataEntry entry = new MetadataEntry("kind", "connector", 42L);

    // then
    assertThat(entry.key()).isEqualTo("kind");
    assertThat(entry.value()).isEqualTo("connector");
    assertThat(entry.valueNumber()).isEqualTo(42L);
  }

  @Test
  void shouldAllowNullValueNumberInMetadataEntry() {
    // when
    final MetadataEntry entry = new MetadataEntry("kind", "connector", null);

    // then
    assertThat(entry.key()).isEqualTo("kind");
    assertThat(entry.value()).isEqualTo("connector");
    assertThat(entry.valueNumber()).isNull();
  }

  @Test
  void shouldSetAndGetMetadataOnEntity() {
    // given
    final MetadataEntry entry = new MetadataEntry("kind", "connector", null);
    final ClusterVariableEntity entity = new ClusterVariableEntity();

    // when
    entity.setMetadata(List.of(entry));

    // then
    assertThat(entity.getMetadata()).containsExactly(entry);
  }

  @Test
  void shouldDifferInEqualsWhenMetadataPresent() {
    // given
    final ClusterVariableEntity withMetadata =
        new ClusterVariableEntity()
            .setId("var-1")
            .setMetadata(List.of(new MetadataEntry("kind", "connector", null)));
    final ClusterVariableEntity withoutMetadata = new ClusterVariableEntity().setId("var-1");

    // then
    assertThat(withMetadata).isNotEqualTo(withoutMetadata);
    assertThat(withMetadata.hashCode()).isNotEqualTo(withoutMetadata.hashCode());
  }

  @Test
  void shouldAllowNullMetadataField() {
    // given
    final ClusterVariableEntity entity = new ClusterVariableEntity();

    // then
    assertThat(entity.getMetadata()).isNull();
  }
}
