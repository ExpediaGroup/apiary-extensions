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

import static com.expediagroup.apiary.extensions.events.metastore.common.Preconditions.checkNotNull;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.booleanProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.intProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.longProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.stringProperty;
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
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.TOPIC;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.io.MetaStoreEventSerDe;

public class KafkaMessageReader implements Iterator<ApiaryListenerEvent>, Closeable {

  private static final Duration POLL_TIMEOUT = Duration.ofHours(1);

  private Configuration conf;
  private KafkaConsumer<Long, byte[]> consumer;
  private MetaStoreEventSerDe eventSerDe;
  private Iterator<ConsumerRecord<Long, byte[]>> records;
  private String topic;

  public KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(conf, eventSerDe, new KafkaConsumer<>(kafkaProperties(conf)));
  }

  @VisibleForTesting
  KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe, KafkaConsumer<Long, byte[]> consumer) {
    this.conf = conf;
    this.consumer = consumer;
    this.eventSerDe = eventSerDe;
    init();
  }

  @VisibleForTesting
  static Properties kafkaProperties(Configuration conf) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS.unprefixedKey(),
      checkNotNull(stringProperty(conf, BOOTSTRAP_SERVERS), "Property " + BOOTSTRAP_SERVERS + " is not set"));
    props.put(GROUP_ID.unprefixedKey(),
      checkNotNull(stringProperty(conf, GROUP_ID), "Property " + GROUP_ID + " is not set"));
    props.put(CLIENT_ID.unprefixedKey(),
      checkNotNull(stringProperty(conf, CLIENT_ID), "Property " + CLIENT_ID + " is not set"));
    props.put(SESSION_TIMEOUT_MS.unprefixedKey(), intProperty(conf, SESSION_TIMEOUT_MS));
    props.put(CONNECTIONS_MAX_IDLE_MS.unprefixedKey(), longProperty(conf, CONNECTIONS_MAX_IDLE_MS));
    props.put(RECONNECT_BACKOFF_MAX_MS.unprefixedKey(), longProperty(conf, RECONNECT_BACKOFF_MAX_MS));
    props.put(RECONNECT_BACKOFF_MS.unprefixedKey(), longProperty(conf, RECONNECT_BACKOFF_MS));
    props.put(RETRY_BACKOFF_MS.unprefixedKey(), longProperty(conf, RETRY_BACKOFF_MS));
    props.put(MAX_POLL_INTERVAL_MS.unprefixedKey(), intProperty(conf, MAX_POLL_INTERVAL_MS));
    props.put(MAX_POLL_RECORDS.unprefixedKey(), intProperty(conf, MAX_POLL_RECORDS));
    props.put(ENABLE_AUTO_COMMIT.unprefixedKey(), booleanProperty(conf, ENABLE_AUTO_COMMIT));
    props.put(AUTO_COMMIT_INTERVAL_MS.unprefixedKey(), intProperty(conf, AUTO_COMMIT_INTERVAL_MS));
    props.put(FETCH_MAX_BYTES.unprefixedKey(), intProperty(conf, FETCH_MAX_BYTES));
    props.put(RECEIVE_BUFFER_BYTES.unprefixedKey(), intProperty(conf, RECEIVE_BUFFER_BYTES));
    props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return props;
  }

  private void init() {
    topic = checkNotNull(stringProperty(conf, TOPIC), "Property " + TOPIC + " is not set");
    consumer.subscribe(Arrays.asList(topic));
  }

  @Override
  public void close() {
    consumer.close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Cannot remove message from Kafka topic");
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

}
