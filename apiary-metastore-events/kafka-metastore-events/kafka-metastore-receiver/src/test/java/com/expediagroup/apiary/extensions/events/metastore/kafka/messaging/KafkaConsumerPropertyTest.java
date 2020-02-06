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

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.AUTO_COMMIT_INTERVAL_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.CONNECTIONS_MAX_IDLE_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.ENABLE_AUTO_COMMIT;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.FETCH_MAX_BYTES;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.GROUP_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.MAX_POLL_INTERVAL_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.MAX_POLL_RECORDS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.RECEIVE_BUFFER_BYTES;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.RECONNECT_BACKOFF_MAX_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.RECONNECT_BACKOFF_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.RETRY_BACKOFF_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.SESSION_TIMEOUT_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.TOPIC_NAME;

import org.junit.Test;

public class KafkaConsumerPropertyTest {

  private static String prefixedKey(String key) {
    return "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging." + key;
  }

  @Test
  public void numberOfProperties() {
    assertThat(KafkaConsumerProperty.values().length).isEqualTo(15);
  }

  @Test
  public void topic() {
    assertThat(TOPIC_NAME.unprefixedKey()).isEqualTo("topic");
    assertThat(TOPIC_NAME.key()).isEqualTo(prefixedKey("topic"));
    assertThat(TOPIC_NAME.defaultValue()).isNull();
  }

  @Test
  public void bootstrapServers() {
    assertThat(BOOTSTRAP_SERVERS.unprefixedKey()).isEqualTo("bootstrap.servers");
    assertThat(BOOTSTRAP_SERVERS.key()).isEqualTo(prefixedKey("bootstrap.servers"));
    assertThat(BOOTSTRAP_SERVERS.defaultValue()).isNull();
  }

  @Test
  public void groupId() {
    assertThat(GROUP_ID.unprefixedKey()).isEqualTo("group.id");
    assertThat(GROUP_ID.key()).isEqualTo(prefixedKey("group.id"));
    assertThat(GROUP_ID.defaultValue()).isNull();
  }

  @Test
  public void clientId() {
    assertThat(CLIENT_ID.unprefixedKey()).isEqualTo("client.id");
    assertThat(CLIENT_ID.key()).isEqualTo(prefixedKey("client.id"));
    assertThat(CLIENT_ID.defaultValue()).isNull();
  }

  @Test
  public void sessionTimeoutMs() {
    assertThat(SESSION_TIMEOUT_MS.unprefixedKey()).isEqualTo("session.timeout.ms");
    assertThat(SESSION_TIMEOUT_MS.key()).isEqualTo(prefixedKey("session.timeout.ms"));
    assertThat(SESSION_TIMEOUT_MS.defaultValue()).isEqualTo(30000);
  }

  @Test
  public void connectionsMaxIdleMs() {
    assertThat(CONNECTIONS_MAX_IDLE_MS.unprefixedKey()).isEqualTo("connections.max.idle.ms");
    assertThat(CONNECTIONS_MAX_IDLE_MS.key()).isEqualTo(prefixedKey("connections.max.idle.ms"));
    assertThat(CONNECTIONS_MAX_IDLE_MS.defaultValue()).isEqualTo(540000L);
  }

  @Test
  public void reconnectBackoffMaxMs() {
    assertThat(RECONNECT_BACKOFF_MAX_MS.unprefixedKey()).isEqualTo("reconnect.backoff.max.ms");
    assertThat(RECONNECT_BACKOFF_MAX_MS.key()).isEqualTo(prefixedKey("reconnect.backoff.max.ms"));
    assertThat(RECONNECT_BACKOFF_MAX_MS.defaultValue()).isEqualTo(1000L);
  }

  @Test
  public void reconnectBackoffMs() {
    assertThat(RECONNECT_BACKOFF_MS.unprefixedKey()).isEqualTo("reconnect.backoff.ms");
    assertThat(RECONNECT_BACKOFF_MS.key()).isEqualTo(prefixedKey("reconnect.backoff.ms"));
    assertThat(RECONNECT_BACKOFF_MS.defaultValue()).isEqualTo(50L);
  }

  @Test
  public void retryBackoffMs() {
    assertThat(RETRY_BACKOFF_MS.unprefixedKey()).isEqualTo("retry.backoff.ms");
    assertThat(RETRY_BACKOFF_MS.key()).isEqualTo(prefixedKey("retry.backoff.ms"));
    assertThat(RETRY_BACKOFF_MS.defaultValue()).isEqualTo(100L);
  }

  @Test
  public void maxPollIntervalMs() {
    assertThat(MAX_POLL_INTERVAL_MS.unprefixedKey()).isEqualTo("max.poll.interval.ms");
    assertThat(MAX_POLL_INTERVAL_MS.key()).isEqualTo(prefixedKey("max.poll.interval.ms"));
    assertThat(MAX_POLL_INTERVAL_MS.defaultValue()).isEqualTo(300000);
  }

  @Test
  public void maxPollRecords() {
    assertThat(MAX_POLL_RECORDS.unprefixedKey()).isEqualTo("max.poll.records");
    assertThat(MAX_POLL_RECORDS.key()).isEqualTo(prefixedKey("max.poll.records"));
    assertThat(MAX_POLL_RECORDS.defaultValue()).isEqualTo(500);
  }

  @Test
  public void enableAutoCommit() {
    assertThat(ENABLE_AUTO_COMMIT.unprefixedKey()).isEqualTo("enable.auto.commit");
    assertThat(ENABLE_AUTO_COMMIT.key()).isEqualTo(prefixedKey("enable.auto.commit"));
    assertThat(ENABLE_AUTO_COMMIT.defaultValue()).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void autoCommitIntervalMs() {
    assertThat(AUTO_COMMIT_INTERVAL_MS.unprefixedKey()).isEqualTo("auto.commit.interval.ms");
    assertThat(AUTO_COMMIT_INTERVAL_MS.key()).isEqualTo(prefixedKey("auto.commit.interval.ms"));
    assertThat(AUTO_COMMIT_INTERVAL_MS.defaultValue()).isEqualTo(5000);
  }

  @Test
  public void fetchMaxBytes() {
    assertThat(FETCH_MAX_BYTES.unprefixedKey()).isEqualTo("fetch.max.bytes");
    assertThat(FETCH_MAX_BYTES.key()).isEqualTo(prefixedKey("fetch.max.bytes"));
    assertThat(FETCH_MAX_BYTES.defaultValue()).isEqualTo(52428800);
  }

  @Test
  public void receiveBufferBytes() {
    assertThat(RECEIVE_BUFFER_BYTES.unprefixedKey()).isEqualTo("receive.buffer.bytes");
    assertThat(RECEIVE_BUFFER_BYTES.key()).isEqualTo(prefixedKey("receive.buffer.bytes"));
    assertThat(RECEIVE_BUFFER_BYTES.defaultValue()).isEqualTo(65536);
  }

}
