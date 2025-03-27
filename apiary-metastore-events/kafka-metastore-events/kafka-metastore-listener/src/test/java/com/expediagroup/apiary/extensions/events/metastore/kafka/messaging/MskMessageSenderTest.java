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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.*;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.MskMessageSender.mskProperties;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.MskMessageSender.topic;

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
public class MskMessageSenderTest {

  public static final String KAFKA_SECURITY_PROTOCOL = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.security.protocol";
  public static final String KAFKA_SASL_MECHANISM = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.sasl.mechanism";
  public static final String KAFKA_JAAS_CONFIG = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.sasl.jaas.config";
  public static final String KAFKA_CALLBACK_HANDLER_CLASS = "com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.sasl.client.callback.handler.class";
  private @Captor ArgumentCaptor<ProducerRecord> producerRecordCaptor;
  private @Mock KafkaMessage kafkaMessage;
  private @Mock KafkaProducer<Long, byte[]> producer;
  private @Mock List<PartitionInfo> partitionInfoList;
  private Configuration conf = new Configuration();

  @Test
  public void send() {
    byte[] payload = { 1, 2, 3 };
    when(kafkaMessage.getPayload()).thenReturn(payload);
    when(kafkaMessage.getQualifiedTableName()).thenReturn("database.table");
    when(producer.partitionsFor("topic")).thenReturn(partitionInfoList);
    when(partitionInfoList.size()).thenReturn(5);
    MskMessageSender kafkaMessageSender = new MskMessageSender("topic", producer);
    kafkaMessageSender.send(kafkaMessage);
    verify(producer).send(producerRecordCaptor.capture());
    ProducerRecord record = producerRecordCaptor.getValue();
    assertThat(record.topic()).isEqualToIgnoringCase("topic");
    assertThat(record.partition()).isEqualTo(1);
    assertThat(record.value()).isEqualTo(payload);
  }
  @Test
  public void populateKafkaPropertiesFromHadoop() {
    conf.set(BOOTSTRAP_SERVERS.key(), "broker");
    conf.set(CLIENT_ID.key(), "client");
    conf.set(ACKS.key(), "acknowledgements");
    conf.set(RETRIES.key(), "1");
    conf.set(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION.key(), "2");
    conf.set(BATCH_SIZE.key(), "3");
    conf.set(LINGER_MS.key(), "4");
    conf.set(BUFFER_MEMORY.key(), "5");
    conf.set(KAFKA_SECURITY_PROTOCOL, "SSL");
    conf.set(KAFKA_SASL_MECHANISM, "AWS_MSK_IAM");
    conf.set(KAFKA_JAAS_CONFIG, "software.amazon.msk.auth.iam.IAMLoginModule required;");
    conf.set(KAFKA_CALLBACK_HANDLER_CLASS, "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    Properties props = mskProperties(conf);
    assertThat(props.get("compression.type")).isEqualTo("none");
    assertThat(props.get("batch.size")).isEqualTo("3");
    assertThat(props.get("acks")).isEqualTo("acknowledgements");
    assertThat(props.get("bootstrap.servers")).isEqualTo("broker");
    assertThat(props.get("buffer.memory")).isEqualTo("5");
    assertThat(props.get("key.serializer")).isEqualTo(LongSerializer.class.getName());
    assertThat(props.get("retries")).isEqualTo("1");
    assertThat(props.get("max.request.size")).isEqualTo(1048576);
    assertThat(props.get("value.serializer")).isEqualTo(ByteArraySerializer.class.getName());
    assertThat(props.get("max.in.flight.requests.per.connection")).isEqualTo("2");
    assertThat(props.get("linger.ms")).isEqualTo("4");
    assertThat(props.get("security.protocol")).isEqualTo("SSL");
    assertThat(props.get("sasl.mechanism")).isEqualTo("AWS_MSK_IAM");
    assertThat(props.get("sasl.jaas.config")).isEqualTo("software.amazon.msk.auth.iam.IAMLoginModule required;");
    assertThat(props.get("sasl.client.callback.handler.class")).isEqualTo("software.amazon.msk.auth.iam.IAMClientCallbackHandler");
  }

  @Test
  public void hadoopTopicIsNotNull() {
    conf.set(TOPIC_NAME.key(), "topic");
    assertThat(topic(conf)).isEqualTo("topic");
  }

  @Test(expected = NullPointerException.class)
  public void topicIsNull() {
    topic(conf);
  }

}
