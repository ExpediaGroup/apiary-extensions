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
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.intProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.longProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.stringProperty;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.ACKS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BATCH_SIZE;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BUFFER_MEMORY;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.CLIENT_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.LINGER_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.RETRIES;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.annotations.VisibleForTesting;

/**
 * Sends messages to a Kafka topic. Note that in order to preserve the
 * order of the events the topic must have only single partition.
 */
public class KafkaMessageSender {

  private final KafkaProducer<Long, byte[]> producer;
  private final String topic;
  private final int numberOfPartitions;

  public KafkaMessageSender(Configuration conf) {
    this(topic(conf), new KafkaProducer<Long, byte[]>(kafkaProperties(conf)));
  }

  @VisibleForTesting
  KafkaMessageSender(String topic, KafkaProducer<Long, byte[]> producer) {
    this.producer = producer;
    this.topic = topic;
    this.numberOfPartitions = producer.partitionsFor(topic).size();
  }

  public void send(KafkaMessage kafkaMessage) {
    int partition = Math.abs(kafkaMessage.getQualifiedTableName().hashCode() % numberOfPartitions);
    producer.send(new ProducerRecord<>(topic, partition, kafkaMessage.getTimestamp(), kafkaMessage.getPayload()));
  }

  @VisibleForTesting
  static Properties kafkaProperties(Configuration conf) {
    Properties props = new Properties();
    props.put(BOOTSTRAP_SERVERS.unprefixedKey(),
        checkNotNull(stringProperty(conf, BOOTSTRAP_SERVERS), "Property " + BOOTSTRAP_SERVERS + " is not set"));
    props.put(CLIENT_ID.unprefixedKey(),
        checkNotNull(stringProperty(conf, CLIENT_ID), "Property " + CLIENT_ID + " is not set"));
    props.put(ACKS.unprefixedKey(), stringProperty(conf, ACKS));
    props.put(RETRIES.unprefixedKey(), intProperty(conf, RETRIES));
    props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.unprefixedKey(),
        intProperty(conf, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION));
    props.put(BATCH_SIZE.unprefixedKey(), intProperty(conf, BATCH_SIZE));
    props.put(LINGER_MS.unprefixedKey(), longProperty(conf, LINGER_MS));
    props.put(BUFFER_MEMORY.unprefixedKey(), longProperty(conf, BUFFER_MEMORY));
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return props;
  }

  @VisibleForTesting
  static String topic(Configuration conf) {
    return checkNotNull(stringProperty(conf, TOPIC), "Property " + TOPIC + " is not set");
  }

}
