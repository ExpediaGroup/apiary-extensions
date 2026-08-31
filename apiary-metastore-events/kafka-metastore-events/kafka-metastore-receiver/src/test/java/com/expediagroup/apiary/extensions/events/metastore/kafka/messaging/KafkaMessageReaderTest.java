/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader.KafkaMessageReaderBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.io.SerDeException;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMessageReaderTest {

  private static final int PARTITION = 0;
  private static final byte[] MESSAGE_CONTENT = "message".getBytes();
  private static final String BOOTSTRAP_SERVERS_STRING = "bootstrap_servers";
  private static final String APPLICATION_NAME = "app";
  private static final String TOPIC_NAME = "topic";

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock KafkaConsumer<Object, byte[]> consumer;
  private @Mock ConsumerRecord<Object, byte[]> message;
  private @Mock ApiaryListenerEvent event;

  private ConsumerRecords<Object, byte[]> messages;
  private KafkaMessageReader reader;
  private SimpleMeterRegistry meterRegistry;

  @Before
  public void init() {
    meterRegistry = new SimpleMeterRegistry();
    List<ConsumerRecord<Object, byte[]>> messageList = ImmutableList.of(message);
    Map<TopicPartition, List<ConsumerRecord<Object, byte[]>>> messageMap = ImmutableMap
        .of(new TopicPartition(TOPIC_NAME, PARTITION), messageList);
    messages = new ConsumerRecords<>(messageMap);
    when(consumer.poll(any(Duration.class))).thenReturn(messages);
    when(message.value()).thenReturn(MESSAGE_CONTENT);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);
    reader = new KafkaMessageReader(TOPIC_NAME, serDe, consumer, meterRegistry);
  }

  @Test
  public void nextReadsRecordsFromQueue() {
    assertThat(reader.next()).isSameAs(event);
    verify(consumer).poll(any(Duration.class));
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test
  public void nextReadsNoRecordsFromQueue() {
    when(consumer.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty()).thenReturn(messages);
    reader.next();
    verify(consumer, times(2)).poll(any(Duration.class));
    verify(serDe).unmarshal(MESSAGE_CONTENT);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void remove() {
    reader.remove();
  }

  @Test
  public void hasNext() {
    assertThat(reader.hasNext()).isTrue();
  }

  @Test
  public void close() {
    reader.close();
    verify(consumer).close();
  }

  @Test(expected = SerDeException.class)
  public void unmarhsallThrowsException() {
    when(serDe.unmarshal(any(byte[].class))).thenThrow(SerDeException.class);
    reader.next();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullBootstrapServers() {
    KafkaMessageReaderBuilder.builder(null, TOPIC_NAME, APPLICATION_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullTopicName() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, null, APPLICATION_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullApplicationNAme() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, null)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyBootstrapServers() {
    KafkaMessageReaderBuilder.builder("", TOPIC_NAME, APPLICATION_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyTopicName() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, "", APPLICATION_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyApplicationNAme() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, "")
        .build();
  }

  @Test
  public void defaultKeyDeserializerIsLong() {
    Properties props = KafkaMessageReaderBuilder
        .builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .buildConsumerProperties();

    assertThat(props.get(KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(LongDeserializer.class.getName());
    assertThat(props.get(VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class.getName());
  }

  @Test
  public void withKeyDeserializerOverridesDefault() {
    Properties props = KafkaMessageReaderBuilder
        .builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .withKeyDeserializer(StringDeserializer.class.getName())
        .buildConsumerProperties();

    assertThat(props.get(KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(StringDeserializer.class.getName());
    assertThat(props.get(VALUE_DESERIALIZER_CLASS_CONFIG)).isEqualTo(ByteArrayDeserializer.class.getName());
  }

  @Test
  public void consumerPropertiesDoNotOverrideKeyDeserializer() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    Properties props = KafkaMessageReaderBuilder
        .builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .withConsumerProperties(consumerProperties)
        .buildConsumerProperties();

    assertThat(props.get(KEY_DESERIALIZER_CLASS_CONFIG)).isEqualTo(LongDeserializer.class.getName());
  }

  @Test
  public void consumerPropertiesWithoutCollisionArePreserved() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

    Properties props = KafkaMessageReaderBuilder
        .builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .withConsumerProperties(consumerProperties)
        .buildConsumerProperties();

    assertThat(props.get(AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
    assertThat(props.get(GROUP_ID_CONFIG)).isEqualTo("apiary-kafka-metastore-receiver-" + APPLICATION_NAME);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullKeyDeserializer() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .withKeyDeserializer(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyKeyDeserializer() {
    KafkaMessageReaderBuilder.builder(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, APPLICATION_NAME)
        .withKeyDeserializer("");
  }

}
