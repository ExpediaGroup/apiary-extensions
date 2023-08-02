/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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

import com.expediagroup.apiary.extensions.events.metastore.common.Property;
import com.expediagroup.apiary.extensions.events.metastore.io.jackson.JsonMetaStoreEventSerDe;

public enum KafkaProducerProperty implements Property {
  TOPIC_NAME("topic.name", null),
  BOOTSTRAP_SERVERS("bootstrap.servers", null),
  CLIENT_ID("client.id", null),
  ACKS("acks", "all"),
  RETRIES("retries", 3),
  MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION("max.in.flight.requests.per.connection", 1),
  BATCH_SIZE("batch.size", 16384),
  LINGER_MS("linger.ms", 1L),
  BUFFER_MEMORY("buffer.memory", 33554432L),
  SERDE_CLASS("serde.class", JsonMetaStoreEventSerDe.class.getName()),
  COMPRESSION_TYPE("compression.type", "none"),
  MAX_REQUEST_SIZE("max.request.size", 1048576);

  private static final String HADOOP_CONF_PREFIX = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.";

  private final String unprefixedKey;
  private final Object defaultValue;

  KafkaProducerProperty(String unprefixedKey, Object defaultValue) {
    this.unprefixedKey = unprefixedKey;
    this.defaultValue = defaultValue;
  }

  @Override
  public String key() {
    return HADOOP_CONF_PREFIX + unprefixedKey;
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
