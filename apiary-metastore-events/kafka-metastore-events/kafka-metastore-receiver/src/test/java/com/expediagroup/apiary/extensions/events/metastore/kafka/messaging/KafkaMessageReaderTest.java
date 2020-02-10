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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
  private static final String GROUP_NAME = "group";
  private static final String TOPIC_NAME = "topic";

  private @Mock MetaStoreEventSerDe serDe;
  private @Mock KafkaConsumer<Long, byte[]> consumer;
  private @Mock ConsumerRecord<Long, byte[]> message;
  private @Mock ApiaryListenerEvent event;

  private ConsumerRecords<Long, byte[]> messages;
  private KafkaMessageReader reader;

  @Before
  public void init() {
    List<ConsumerRecord<Long, byte[]>> messageList = ImmutableList.of(message);
    Map<TopicPartition, List<ConsumerRecord<Long, byte[]>>> messageMap = ImmutableMap
        .of(new TopicPartition(TOPIC_NAME, PARTITION), messageList);
    messages = new ConsumerRecords<>(messageMap);
    when(consumer.poll(any(Duration.class))).thenReturn(messages);
    when(message.value()).thenReturn(MESSAGE_CONTENT);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);
    reader = new KafkaMessageReader(TOPIC_NAME, serDe, consumer);
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingBootstrapServers() {
    KafkaMessageReader.Builder.aKafkaMessageReader("", TOPIC_NAME, GROUP_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingTopicName() {
    KafkaMessageReader.Builder.aKafkaMessageReader(BOOTSTRAP_SERVERS_STRING, "", GROUP_NAME)
        .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingGroupId() {
    KafkaMessageReader.Builder.aKafkaMessageReader(BOOTSTRAP_SERVERS_STRING, TOPIC_NAME, "")
        .build();
  }

  @Test
  public void close() {
    reader.close();
    verify(consumer).close();
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

  @Test(expected = SerDeException.class)
  public void unmarhsallThrowsException() {
    when(serDe.unmarshal(any(byte[].class))).thenThrow(SerDeException.class);
    reader.next();
  }

}
