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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageSender.kafkaProperties;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageSender.topic;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.ACKS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BATCH_SIZE;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BUFFER_MEMORY;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.LINGER_MS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.RETRIES;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.datanucleus.store.types.wrappers.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageSenderTest {

  private @Captor ArgumentCaptor<ProducerRecord> producerRecordCaptor;
  private @Mock KafkaMessage kafkaMessage;
  private @Mock KafkaProducer<Long, byte[]> producer;
  private @Mock List<PartitionInfo> partitionInfoList;
  private static final String TOPIC_NAME = "topic";
  private Configuration conf = new Configuration();

  @Test
  public void send() {
    byte[] payload = { 1, 2, 3 };
    when(kafkaMessage.getPayload()).thenReturn(payload);
    when(kafkaMessage.getQualifiedTableName()).thenReturn("database.table");
    when(producer.partitionsFor(TOPIC_NAME)).thenReturn(partitionInfoList);
    when(partitionInfoList.size()).thenReturn(5);
    KafkaMessageSender kafkaMessageSender = new KafkaMessageSender(TOPIC_NAME, producer);
    kafkaMessageSender.send(kafkaMessage);
    verify(producer).send(producerRecordCaptor.capture());
    ProducerRecord record = producerRecordCaptor.getValue();
    assertThat(record.topic()).isEqualToIgnoringCase(TOPIC_NAME);
    assertThat(record.partition()).isEqualTo(1);
    assertThat(record.value()).isEqualTo(payload);
  }

  @Test
  public void populateKafkaProperties() {
    conf.set(BOOTSTRAP_SERVERS.hadoopConfKey(), "broker");
    conf.set(ACKS.hadoopConfKey(), "acknowledgements");
    conf.set(RETRIES.hadoopConfKey(), "1");
    conf.set(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.hadoopConfKey(), "2");
    conf.set(BATCH_SIZE.hadoopConfKey(), "3");
    conf.set(LINGER_MS.hadoopConfKey(), "4");
    conf.set(BUFFER_MEMORY.hadoopConfKey(), "5");
    Properties props = kafkaProperties(conf);
    assertThat(props.get("bootstrap.servers")).isEqualTo("broker");
    assertThat(props.get("acks")).isEqualTo("acknowledgements");
    assertThat(props.get("retries")).isEqualTo(1);
    assertThat(props.get("max.in.flight.requests.per.connection")).isEqualTo(2);
    assertThat(props.get("batch.size")).isEqualTo(3);
    assertThat(props.get("linger.ms")).isEqualTo(4L);
    assertThat(props.get("buffer.memory")).isEqualTo(5L);
    assertThat(props.get("key.serializer")).isEqualTo(LongSerializer.class.getName());
    assertThat(props.get("value.serializer")).isEqualTo(ByteArraySerializer.class.getName());
  }

  @Test
  public void topicIsNotNull() {
    conf.set(TOPIC.hadoopConfKey(), TOPIC_NAME);
    assertThat(topic(conf)).isEqualTo(TOPIC_NAME);
  }

  @Test(expected = NullPointerException.class)
  public void topicIsNull() {
    topic(conf);
  }

}
