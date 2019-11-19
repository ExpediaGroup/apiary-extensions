/**
 * Copyright (C) 2018-2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expediagroup.apiary.extensions.events.metastore.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class KafkaMessageTest {

  @Test
  public void typical() {
    byte[] payload = { 1, 2, 3 };
    long timestamp = 123l;
    String database = " database ";
    String table = " table ";
    KafkaMessage message = KafkaMessage.builder()
      .database(database)
      .table(table)
      .payload(payload)
      .timestamp(timestamp)
      .build();
    assertThat(message.getDatabase()).isEqualTo(database.trim());
    assertThat(message.getTable()).isEqualTo(table.trim());
    assertThat(message.getPayload()).isEqualTo(payload);
    assertThat(message.getTimestamp()).isEqualTo(timestamp);
    assertThat(message.getQualifiedTableName()).isEqualTo(database.trim() + "." + table.trim());
  }

  @Test
  public void emptyTableThrowsException() {
    byte[] payload = { 1, 2, 3 };
    long timestamp = 123l;
    String database = " database ";
    String table = " ";
    assertThatThrownBy(() -> KafkaMessage.builder()
      .database(database)
      .table(table)
      .payload(payload)
      .timestamp(timestamp)
      .build()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void emptyDatabaseThrowsException() {
    byte[] payload = { 1, 2, 3 };
    long timestamp = 123l;
    String database = "  ";
    String table = " table ";
    assertThatThrownBy(() -> KafkaMessage.builder()
      .database(database)
      .table(table)
      .payload(payload)
      .timestamp(timestamp)
      .build()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void nullPayloadThrowsException() {
    long timestamp = 123l;
    String database = " database ";
    String table = " table ";
    assertThatThrownBy(() -> KafkaMessage.builder()
      .database(database)
      .table(table)
      .timestamp(timestamp)
      .build()).isInstanceOf(NullPointerException.class);
  }

}
