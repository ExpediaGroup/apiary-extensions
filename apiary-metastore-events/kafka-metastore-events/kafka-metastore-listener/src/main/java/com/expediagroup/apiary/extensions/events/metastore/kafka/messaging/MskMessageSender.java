/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.stringProperty;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.*;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.HADOOP_CONF_PREFIX;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
/**
 * Sends messages to a Kafka topic. Note that in order to preserve the
 * order of the events the topic must have only single partition.
 */
public class MskMessageSender {

  private static final Logger log = LoggerFactory.getLogger(MskMessageSender.class);

  private final KafkaProducer<Long, byte[]> producer;
  private final String topic;
  private final int numberOfPartitions;

  public MskMessageSender(Configuration conf) {
    this(topic(conf), new KafkaProducer<>(mskProperties(conf)));
  }

  @VisibleForTesting
  MskMessageSender(String topic, KafkaProducer<Long, byte[]> producer) {
    this.producer = producer;
    this.topic = topic;
    this.numberOfPartitions = producer.partitionsFor(topic).size();
    log.debug("ID_DEBUG: PARTITIONS_1, partitions: {} for topic: {}", numberOfPartitions, topic);
  }

  public void send(KafkaMessage kafkaMessage) {
    int partition = Math.abs(kafkaMessage.getQualifiedTableName().hashCode() % numberOfPartitions);
    producer.send(new ProducerRecord<>(topic, partition, kafkaMessage.getTimestamp(), kafkaMessage.getPayload()));
  }

  @VisibleForTesting
  static Properties mskProperties(Configuration conf) {
    Properties props = new Properties();

    conf.forEach(entry -> {
      if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
        props.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
      }
    });

    props.put(CLIENT_ID.unprefixedKey(), (stringProperty(conf, CLIENT_ID) + "-msk"));

    String mskBootstrapServers = System.getenv("MSK_BOOTSTRAP_SERVERS");
    if (StringUtils.isNotEmpty(mskBootstrapServers)) {
      props.put(BOOTSTRAP_SERVERS.unprefixedKey(), mskBootstrapServers);
    }

    props.put(COMPRESSION_TYPE.unprefixedKey(), stringProperty(conf, COMPRESSION_TYPE));
    props.put(MAX_REQUEST_SIZE.unprefixedKey(), intProperty(conf, MAX_REQUEST_SIZE));
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    return props;
  }

  @VisibleForTesting
  static String topic(Configuration conf) {
    return checkNotNull(stringProperty(conf, TOPIC_NAME), "Property " + TOPIC_NAME + " is not set");
  }

}
