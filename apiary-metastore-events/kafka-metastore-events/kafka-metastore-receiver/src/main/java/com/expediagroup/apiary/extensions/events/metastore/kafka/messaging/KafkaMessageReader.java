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

import static com.expediagroup.apiary.extensions.events.metastore.common.Preconditions.checkNotEmpty;
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

public class KafkaMessageReader implements Iterator<ApiaryListenerEvent>, Closeable {

  private static final Duration POLL_TIMEOUT = Duration.ofHours(1);

  private KafkaConsumer<Long, byte[]> consumer;
  private MetaStoreEventSerDe eventSerDe;
  private Iterator<ConsumerRecord<Long, byte[]>> records;

  public KafkaMessageReader(MetaStoreEventSerDe eventSerDe, String topicName, Properties properties) {
    this(eventSerDe, topicName, new KafkaConsumer<>(properties));
  }

  @VisibleForTesting
  KafkaMessageReader(MetaStoreEventSerDe eventSerDe, String topicName, KafkaConsumer<Long, byte[]> consumer) {
    this.eventSerDe = eventSerDe;
    this.consumer = consumer;
    this.consumer.subscribe(Collections.singletonList(checkNotEmpty(topicName, "Topic name is not set.")));
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

  public static final class KafkaConsumerPropertiesBuilder {

    private String bootstrapServers;
    private String groupId;
    private String clientId;
    private int sessionTimeoutMillis = (int) SESSION_TIMEOUT_MS.defaultValue();
    private long connectionsMaxIdleMillis = (long) CONNECTIONS_MAX_IDLE_MS.defaultValue();
    private long reconnectBackoffMaxMillis = (long) RECONNECT_BACKOFF_MAX_MS.defaultValue();
    private long reconnectBackoffMillis = (long) RECONNECT_BACKOFF_MS.defaultValue();
    private long retryBackoffMillis = (long) RETRY_BACKOFF_MS.defaultValue();
    private int maxPollIntervalMillis = (int) MAX_POLL_INTERVAL_MS.defaultValue();
    private int maxPollRecords = (int) MAX_POLL_RECORDS.defaultValue();
    private boolean enableAutoCommit = (boolean) ENABLE_AUTO_COMMIT.defaultValue();
    private int autoCommitIntervalMillis = (int) AUTO_COMMIT_INTERVAL_MS.defaultValue();
    private int fetchMaxBytes = (int) FETCH_MAX_BYTES.defaultValue();
    private int receiveBufferBytes = (int) RECEIVE_BUFFER_BYTES.defaultValue();

    private KafkaConsumerPropertiesBuilder(String bootstrapServers, String groupId, String clientId) {
      this.bootstrapServers = bootstrapServers;
      this.groupId = groupId;
      this.clientId = clientId;
    }

    public static KafkaConsumerPropertiesBuilder aKafkaConsumerProperties(String bootstrapServers, String groupId,
        String clientId) {
      return new KafkaConsumerPropertiesBuilder(bootstrapServers, groupId, clientId);
    }

    public KafkaConsumerPropertiesBuilder withSessionTimeoutMillis(int sessionTimeoutMillis) {
      this.sessionTimeoutMillis = sessionTimeoutMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withConnectionsMaxIdleMillis(long connectionsMaxIdleMillis) {
      this.connectionsMaxIdleMillis = connectionsMaxIdleMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withReconnectBackoffMaxMillis(long reconnectBackoffMaxMillis) {
      this.reconnectBackoffMaxMillis = reconnectBackoffMaxMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withReconnectBackoffMillis(long reconnectBackoffMillis) {
      this.reconnectBackoffMillis = reconnectBackoffMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withRetryBackoffMillis(long retryBackoffMillis) {
      this.retryBackoffMillis = retryBackoffMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withMaxPollIntervalMillis(int maxPollIntervalMillis) {
      this.maxPollIntervalMillis = maxPollIntervalMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withMaxPollRecords(int maxPollRecords) {
      this.maxPollRecords = maxPollRecords;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withEnableAutoCommit(boolean enableAutoCommit) {
      this.enableAutoCommit = enableAutoCommit;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withAutoCommitIntervalMillis(int autoCommitIntervalMillis) {
      this.autoCommitIntervalMillis = autoCommitIntervalMillis;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withFetchMaxBytes(int fetchMaxBytes) {
      this.fetchMaxBytes = fetchMaxBytes;
      return this;
    }

    public KafkaConsumerPropertiesBuilder withReceiveBufferBytes(int receiveBufferBytes) {
      this.receiveBufferBytes = receiveBufferBytes;
      return this;
    }

    public Properties build() {
      Properties props = new Properties();
      props.put(BOOTSTRAP_SERVERS.unprefixedKey(),
          checkNotEmpty(bootstrapServers, "Property " + BOOTSTRAP_SERVERS + " is not set"));
      props.put(GROUP_ID.unprefixedKey(), checkNotEmpty(groupId, "Property " + GROUP_ID + " is not set"));
      props.put(CLIENT_ID.unprefixedKey(), checkNotEmpty(clientId, "Property " + CLIENT_ID + " is not set"));
      props.put(SESSION_TIMEOUT_MS.unprefixedKey(), sessionTimeoutMillis);
      props.put(CONNECTIONS_MAX_IDLE_MS.unprefixedKey(), connectionsMaxIdleMillis);
      props.put(RECONNECT_BACKOFF_MAX_MS.unprefixedKey(), reconnectBackoffMaxMillis);
      props.put(RECONNECT_BACKOFF_MS.unprefixedKey(), reconnectBackoffMillis);
      props.put(RETRY_BACKOFF_MS.unprefixedKey(), retryBackoffMillis);
      props.put(MAX_POLL_INTERVAL_MS.unprefixedKey(), maxPollIntervalMillis);
      props.put(MAX_POLL_RECORDS.unprefixedKey(), maxPollRecords);
      props.put(ENABLE_AUTO_COMMIT.unprefixedKey(), enableAutoCommit);
      props.put(AUTO_COMMIT_INTERVAL_MS.unprefixedKey(), autoCommitIntervalMillis);
      props.put(FETCH_MAX_BYTES.unprefixedKey(), fetchMaxBytes);
      props.put(RECEIVE_BUFFER_BYTES.unprefixedKey(), receiveBufferBytes);
      props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      return props;
    }
  }
}
