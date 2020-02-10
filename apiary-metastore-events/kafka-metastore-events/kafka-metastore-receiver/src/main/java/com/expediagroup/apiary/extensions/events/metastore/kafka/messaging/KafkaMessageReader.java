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

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import static com.expediagroup.apiary.extensions.events.metastore.common.Preconditions.checkNotEmpty;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.io.jackson.JsonMetaStoreEventSerDe;

public class KafkaMessageReader implements Iterator<ApiaryListenerEvent>, Closeable {

  private static final Duration POLL_TIMEOUT = Duration.ofMinutes(5);

  private KafkaConsumer<Long, byte[]> consumer;
  private MetaStoreEventSerDe eventSerDe;
  private Iterator<ConsumerRecord<Long, byte[]>> records;

  private KafkaMessageReader(String topicName, MetaStoreEventSerDe eventSerDe, Properties consumerProperties) {
    this(topicName, eventSerDe, new KafkaConsumer(consumerProperties));
  }

  @VisibleForTesting
  KafkaMessageReader(String topicName, MetaStoreEventSerDe eventSerDe, KafkaConsumer<Long, byte[]> consumer) {
    this.eventSerDe = eventSerDe;
    this.consumer = consumer;
    this.consumer.subscribe(Collections.singletonList(topicName));
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from Kafka topic.");
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public ApiaryListenerEvent next() {
    readRecordsIfNeeded();
    ConsumerRecord<Long, byte[]> next = records.next();
    return eventSerDe.unmarshal(next.value());
  }

  private void readRecordsIfNeeded() {
    while (records == null || !records.hasNext()) {
      records = consumer.poll(POLL_TIMEOUT).iterator();
    }
  }

  public static final class Builder {

    private String bootstrapServers;
    private String topicName;
    private String groupId;
    private String clientId = "ApiaryMetastoreReceiver";
    private MetaStoreEventSerDe metaStoreEventSerDe = new JsonMetaStoreEventSerDe();
    private Properties consumerProperties = new Properties();

    private Builder(String bootstrapServers, String topicName, String groupId) {
      this.bootstrapServers = bootstrapServers;
      this.topicName = topicName;
      this.groupId = groupId;
    }

    public static Builder aKafkaMessageReader(String bootstrapServers, String topicName, String groupId) {
      return new Builder(bootstrapServers, topicName, groupId);
    }

    public Builder withClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder withMetaStoreEventSerDe(MetaStoreEventSerDe metaStoreEventSerDe) {
      this.metaStoreEventSerDe = metaStoreEventSerDe;
      return this;
    }

    public Builder withConsumerProperties(Properties consumerProperties) {
      this.consumerProperties = consumerProperties;
      return this;
    }

    public KafkaMessageReader build() {
      Properties props = new Properties();
      props.put(BOOTSTRAP_SERVERS_CONFIG,
          checkNotEmpty(bootstrapServers, "Property " + BOOTSTRAP_SERVERS_CONFIG + " is not set"));
      props.put(GROUP_ID_CONFIG, checkNotEmpty(groupId, "Property " + GROUP_ID_CONFIG + " is not set"));
      props.put(CLIENT_ID_CONFIG, clientId);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      consumerProperties.forEach(
          (key, value) -> props.merge(key, value, (v1, v2) -> v1)
      );
      return new KafkaMessageReader(checkNotEmpty(topicName, "Topic name is not set."), metaStoreEventSerDe, props);
    }
  }
}
