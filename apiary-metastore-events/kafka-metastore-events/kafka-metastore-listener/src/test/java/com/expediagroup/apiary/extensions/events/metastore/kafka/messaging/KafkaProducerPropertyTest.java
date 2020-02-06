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
package com.expediagroup.apiary.extensions.events.metastore.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.ACKS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BATCH_SIZE;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BUFFER_MEMORY;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.LINGER_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.RETRIES;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.SERDE_CLASS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC;

import org.junit.Test;

import com.expediagroup.apiary.extensions.events.metastore.io.jackson.JsonMetaStoreEventSerDe;

public class KafkaProducerPropertyTest {

  private static String hadoopPrefixedKey(String key) {
    return "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging." + key;
  }

  private static String kafkaPrefixedKey(String key) {
    return "KAFKA_" + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KafkaProducerProperty.values().length).isEqualTo(10);
  }

  @Test
  public void topic() {
    assertThat(TOPIC.unprefixedKey()).isEqualTo("topic");
    assertThat(TOPIC.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("topic"));
    assertThat(TOPIC.environmentKey()).isEqualTo(kafkaPrefixedKey("TOPIC"));
    assertThat(TOPIC.defaultValue()).isNull();
  }

  @Test
  public void bootstrapServers() {
    assertThat(BOOTSTRAP_SERVERS.unprefixedKey()).isEqualTo("bootstrap.servers");
    assertThat(BOOTSTRAP_SERVERS.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("bootstrap.servers"));
    assertThat(BOOTSTRAP_SERVERS.environmentKey()).isEqualTo(kafkaPrefixedKey("BOOTSTRAP_SERVERS"));
    assertThat(BOOTSTRAP_SERVERS.defaultValue()).isNull();
  }

  @Test
  public void clientId() {
    assertThat(CLIENT_ID.unprefixedKey()).isEqualTo("client.id");
    assertThat(CLIENT_ID.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("client.id"));
    assertThat(CLIENT_ID.environmentKey()).isEqualTo(kafkaPrefixedKey("CLIENT_ID"));
    assertThat(CLIENT_ID.defaultValue()).isNull();
  }

  @Test
  public void acks() {
    assertThat(ACKS.unprefixedKey()).isEqualTo("acks");
    assertThat(ACKS.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("acks"));
    assertThat(ACKS.environmentKey()).isEqualTo(kafkaPrefixedKey("ACKS"));
    assertThat(ACKS.defaultValue()).isEqualTo("all");
  }

  @Test
  public void retires() {
    assertThat(RETRIES.unprefixedKey()).isEqualTo("retries");
    assertThat(RETRIES.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("retries"));
    assertThat(RETRIES.environmentKey()).isEqualTo(kafkaPrefixedKey("RETRIES"));
    assertThat(RETRIES.defaultValue()).isEqualTo(3);
  }

  @Test
  public void maxInFlightRequestsPerConnection() {
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.unprefixedKey())
        .isEqualTo("max.in.flight.requests.per.connection");
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.hadoopConfKey())
        .isEqualTo(hadoopPrefixedKey("max.in.flight.requests.per.connection"));
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.environmentKey())
        .isEqualTo(kafkaPrefixedKey("MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION"));
    assertThat(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.defaultValue()).isEqualTo(1);
  }

  @Test
  public void batchSize() {
    assertThat(BATCH_SIZE.unprefixedKey()).isEqualTo("batch.size");
    assertThat(BATCH_SIZE.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("batch.size"));
    assertThat(BATCH_SIZE.environmentKey()).isEqualTo(kafkaPrefixedKey("BATCH_SIZE"));
    assertThat(BATCH_SIZE.defaultValue()).isEqualTo(16384);
  }

  @Test
  public void lingerMs() {
    assertThat(LINGER_MS.unprefixedKey()).isEqualTo("linger.ms");
    assertThat(LINGER_MS.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("linger.ms"));
    assertThat(LINGER_MS.environmentKey()).isEqualTo(kafkaPrefixedKey("LINGER_MS"));
    assertThat(LINGER_MS.defaultValue()).isEqualTo(1L);
  }

  @Test
  public void bufferMemory() {
    assertThat(BUFFER_MEMORY.unprefixedKey()).isEqualTo("buffer.memory");
    assertThat(BUFFER_MEMORY.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("buffer.memory"));
    assertThat(BUFFER_MEMORY.environmentKey()).isEqualTo(kafkaPrefixedKey("BUFFER_MEMORY"));
    assertThat(BUFFER_MEMORY.defaultValue()).isEqualTo(33554432L);
  }

  @Test
  public void serdeClass() {
    assertThat(SERDE_CLASS.unprefixedKey()).isEqualTo("serde.class");
    assertThat(SERDE_CLASS.hadoopConfKey()).isEqualTo(hadoopPrefixedKey("serde.class"));
    assertThat(SERDE_CLASS.environmentKey()).isEqualTo(kafkaPrefixedKey("SERDE_CLASS"));
    assertThat(SERDE_CLASS.defaultValue()).isEqualTo(JsonMetaStoreEventSerDe.class.getName());
  }

}
