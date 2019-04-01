/**
 * Copyright (C) 2018-2019 Expedia Inc.
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
package com.expedia.apiary.extensions.receiver.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.ImmutableList;

import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.sqs.messaging.DefaultSqsMessageDeserializer;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageReader;
import com.expedia.apiary.extensions.receiver.sqs.model.SqsMessageProperty;

@RunWith(MockitoJUnitRunner.class)
public class SqsMessageReaderTest {

  private static final String QUEUE_NAME = "queue";
  private static final Integer WAIT_TIME = 1;
  private static final Integer VISIBILITY_TIMEOUT = 2;
  private static final Integer MAX_MESSAGES = 3;
  private static final String RECEIPT_HANDLER = "receipt_handler";
  private static final String MESSAGE_CONTENT = "message";

  private @Mock DefaultSqsMessageDeserializer serDe;
  private @Mock AmazonSQS consumer;
  private @Mock ReceiveMessageResult receiveMessageResult;
  private @Mock List<Message> messages;
  private @Mock Iterator<Message> messageIterator;
  private @Mock Message message;
  private @Mock ListenerEvent event;

  private @Captor ArgumentCaptor<ReceiveMessageRequest> receiveMessageRequestCaptor;
  private @Captor ArgumentCaptor<DeleteMessageRequest> deleteMessageRequestCaptor;
  private @Captor ArgumentCaptor<ChangeMessageVisibilityRequest> changeMessageVisibilityRequestCaptor;

  private final Configuration conf = new Configuration();
  private SqsMessageReader reader;

  @Before
  public void init() throws Exception {
    when(consumer.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResult);
    when(receiveMessageResult.getMessages()).thenReturn(messages);
    when(messages.iterator()).thenReturn(messageIterator);
    when(messageIterator.hasNext()).thenReturn(true);
    when(messageIterator.next()).thenReturn(message);
    when(message.getReceiptHandle()).thenReturn(RECEIPT_HANDLER);
    when(message.getBody()).thenReturn(MESSAGE_CONTENT);
    when(serDe.unmarshal(MESSAGE_CONTENT)).thenReturn(event);

    reader = new SqsMessageReader.Builder(QUEUE_NAME)
        .withConsumer(consumer)
        .withMessageDeserializer(serDe)
        .withMaxMessages(MAX_MESSAGES)
        .withWaitTimeSeconds(WAIT_TIME)
        .withVisibilityTimeoutSeconds(VISIBILITY_TIMEOUT)
        .build();
  }

  @Test
  public void close() throws Exception {
    reader.close();
    verify(consumer).shutdown();
  }

  @Test
  public void readRecordsFromQueue() throws Exception {
    MessageEvent messageEvent = reader.read().get();
    ListenerEvent result = messageEvent.getEvent();
    assertThat(result).isSameAs(this.event);
    verify(consumer).receiveMessage(receiveMessageRequestCaptor.capture());
    assertThat(receiveMessageRequestCaptor.getValue().getQueueUrl()).isEqualTo(QUEUE_NAME);
    assertThat(receiveMessageRequestCaptor.getValue().getWaitTimeSeconds()).isEqualTo(WAIT_TIME);
    assertThat(receiveMessageRequestCaptor.getValue().getMaxNumberOfMessages()).isEqualTo(MAX_MESSAGES);
    assertThat(receiveMessageRequestCaptor.getValue().getVisibilityTimeout()).isEqualTo(VISIBILITY_TIMEOUT);
    verify(serDe).unmarshal(MESSAGE_CONTENT);
    Map<MessageProperty, String> messageDetails = messageEvent.getMessageProperties();
    assertThat(messageDetails.get(SqsMessageProperty.RECEIPT_HANDLE)).isEqualTo(RECEIPT_HANDLER);
  }

  @Test
  public void readNoRecordsFromQueue() throws Exception {
    when(receiveMessageResult.getMessages()).thenReturn(ImmutableList.<Message>of());
    Optional<MessageEvent> result = reader.read();
    verify(consumer, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
    verify(serDe, times(0)).unmarshal(any());
    assertThat(result.isPresent()).isEqualTo(false);
  }

  @Test
  public void deleteMessageFromQueue() throws Exception {
    MessageEvent messageEvent = reader.read().get();
    reader.delete(messageEvent.getMessageProperties().get(SqsMessageProperty.RECEIPT_HANDLE));
    verify(consumer).deleteMessage(deleteMessageRequestCaptor.capture());
    assertThat(deleteMessageRequestCaptor.getValue().getQueueUrl()).isEqualTo(QUEUE_NAME);
    assertThat(deleteMessageRequestCaptor.getValue().getReceiptHandle()).isEqualTo(RECEIPT_HANDLER);
  }

  @Test
  public void extendVisibilityTimeout() throws Exception {
    MessageEvent messageEvent = reader.read().get();
    reader.extendVisibilityTimeout(messageEvent.getMessageProperties().get(SqsMessageProperty.RECEIPT_HANDLE));
    verify(consumer).changeMessageVisibility(changeMessageVisibilityRequestCaptor.capture());
    assertThat(changeMessageVisibilityRequestCaptor.getValue().getQueueUrl()).isEqualTo(QUEUE_NAME);
    assertThat(changeMessageVisibilityRequestCaptor.getValue().getReceiptHandle()).isEqualTo(RECEIPT_HANDLER);
    assertThat(changeMessageVisibilityRequestCaptor.getValue().getVisibilityTimeout()).isEqualTo(VISIBILITY_TIMEOUT);
  }

  @Test(expected = SerDeException.class)
  public void unmarshallThrowsException() throws Exception {
    when(serDe.unmarshal(any(String.class))).thenThrow(RuntimeException.class);
    reader.read();
  }
}
