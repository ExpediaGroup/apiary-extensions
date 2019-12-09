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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.expediagroup.apiary.extensions.events.metastore.common.Property;

public enum KafkaConsumerProperty implements Property {
  TOPIC("topic", null),
  BOOTSTRAP_SERVERS("bootstrap.servers", null),
  GROUP_ID("group.id", null),
  CLIENT_ID("client.id", "ApiaryKafkaMetaStoreReceiver"),
  SESSION_TIMEOUT_MS("session.timeout.ms", 30000),
  CONNECTIONS_MAX_IDLE_MS("connections.max.idle.ms", MINUTES.toMillis(9)),
  RECONNECT_BACKOFF_MAX_MS("reconnect.backoff.max.ms", SECONDS.toMillis(1)),
  RECONNECT_BACKOFF_MS("reconnect.backoff.ms", 50L),
  RETRY_BACKOFF_MS("retry.backoff.ms", 100L),
  MAX_POLL_INTERVAL_MS("max.poll.interval.ms", 300000),
  MAX_POLL_RECORDS("max.poll.records", 500),
  ENABLE_AUTO_COMMIT("enable.auto.commit", true),
  AUTO_COMMIT_INTERVAL_MS("auto.commit.interval.ms", 5000),
  FETCH_MAX_BYTES("fetch.max.bytes", 52428800),
  RECEIVE_BUFFER_BYTES("receive.buffer.bytes", 65536);

  private static final String PROPERTY_PREFIX = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.";

  private final String unprefixedKey;
  private final Object defaultValue;

  private KafkaConsumerProperty(String unprefixedKey, Object defaultValue) {
    this.unprefixedKey = unprefixedKey;
    this.defaultValue = defaultValue;
  }

  @Override
  public String key() {
    return new StringBuffer(PROPERTY_PREFIX).append(unprefixedKey).toString();
  }

  @Override
  public String unprefixedKey() {
    return unprefixedKey;
  }

  @Override
  public Object defaultValue() {
    return defaultValue;
  }

  @Override
  public String toString() {
    return key();
  }

}
