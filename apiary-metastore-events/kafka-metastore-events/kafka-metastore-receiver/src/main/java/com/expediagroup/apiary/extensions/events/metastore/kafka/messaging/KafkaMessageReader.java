/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

import static com.expediagroup.apiary.extensions.events.metastore.kafka.common.Preconditions.checkNotNull;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.common.PropertyUtils.stringProperty;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.TOPIC;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.MetaStoreEventSerDe;

public class KafkaMessageReader implements Iterator<SerializableListenerEvent>, Closeable {

  private String topic;
  private final Configuration conf;
  private final KafkaConsumer<Long, byte[]> consumer;
  private final MetaStoreEventSerDe eventSerDe;
  private Iterator<ConsumerRecord<Long, byte[]>> records;

  public KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe) {
    this(conf, eventSerDe, new HiveMetaStoreEventKafkaConsumer(conf));
  }

  @VisibleForTesting
  KafkaMessageReader(Configuration conf, MetaStoreEventSerDe eventSerDe, KafkaConsumer<Long, byte[]> consumer) {
    this.conf = conf;
    this.consumer = consumer;
    this.eventSerDe = eventSerDe;
    init();
  }

  private void init() {
    topic = checkNotNull(stringProperty(conf, TOPIC), "Property " + TOPIC + " is not set");
    consumer.subscribe(Arrays.asList(topic));
  }

  @Override
  public void close() throws IOException {
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
  public SerializableListenerEvent next() {
    readRecordsIfNeeded();
    ConsumerRecord<Long, byte[]> next = records.next();
    return eventSerDe.unmarshal(next.value());
  }

  private void readRecordsIfNeeded() {
    while (records == null || !records.hasNext()) {
      records = consumer.poll(Long.MAX_VALUE).iterator();
    }
  }

}
