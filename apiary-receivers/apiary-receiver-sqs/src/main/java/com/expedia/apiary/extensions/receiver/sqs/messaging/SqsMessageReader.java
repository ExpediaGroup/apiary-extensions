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
package com.expedia.apiary.extensions.receiver.sqs.messaging;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.JsonMetaStoreEventDeserializer;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageDeserializer;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageReader;

public class SqsMessageReader implements MessageReader {
  private static final Integer DEFAULT_POLLING_WAIT_TIME_SECONDS = 10;
  private static final Integer DEFAULT_MAX_MESSAGES = 10;

  private String queueUrl;
  private Integer waitTimeSeconds;
  private Integer maxMessages;
  private MessageDeserializer messageDeserializer;
  private AmazonSQS consumer;
  private Iterator<Message> records;

  private SqsMessageReader(String queueUrl, int waitTimeSeconds, int maxMessages,
                           MessageDeserializer messageDeserializer, AmazonSQS consumer) {
    this.queueUrl = queueUrl;
    this.waitTimeSeconds = waitTimeSeconds;
    this.maxMessages = maxMessages;
    this.messageDeserializer = messageDeserializer;
    this.consumer = consumer;
  }

  @Override
  public void close() {
    consumer.shutdown();
  }

  @Override
  public Optional<MessageEvent> read() {
    if (records == null || !records.hasNext()) {
      records = receiveMessage();
    }
    if (records.hasNext()) {
      Message message = records.next();
      MessageEvent sqsMessageEvent = messageEvent(message);
      return Optional.of(sqsMessageEvent);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public void delete(MessageEvent messageEvent) {
    String receiptHandle = messageEvent.getMessageProperties()
        .get(SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE);
    DeleteMessageRequest request = new DeleteMessageRequest()
        .withQueueUrl(queueUrl)
        .withReceiptHandle(receiptHandle);
    consumer.deleteMessage(request);
  }

  private Iterator<Message> receiveMessage() {
      ReceiveMessageRequest request = new ReceiveMessageRequest()
          .withQueueUrl(queueUrl)
          .withWaitTimeSeconds(waitTimeSeconds)
          .withMaxNumberOfMessages(maxMessages);
      return consumer.receiveMessage(request).getMessages().iterator();
  }

  private MessageEvent messageEvent(Message message) {
    ListenerEvent listenerEvent = eventPayLoad(message);
    Map<MessageProperty, String> properties = Collections.singletonMap(
        SqsMessageProperty.SQS_MESSAGE_RECEIPT_HANDLE, message.getReceiptHandle());
    return new MessageEvent(listenerEvent, properties);
  }

  private ListenerEvent eventPayLoad(Message message) {
    try {
      return messageDeserializer.unmarshal(message.getBody());
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

  public static final class Builder {
    private String queueUrl;
    private Integer waitTimeSeconds;
    private Integer maxMessages;
    private AmazonSQS consumer;
    private MessageDeserializer messageDeserializer;

    public Builder(String queueUrl) {
      this.queueUrl = queueUrl;
    }

    public Builder withConsumer(AmazonSQS consumer) {
      this.consumer = consumer;
      return this;
    }

    public Builder withMessageDeserializer(MessageDeserializer messageDeserializer) {
      this.messageDeserializer = messageDeserializer;
      return this;
    }

    public Builder withWaitTimeSeconds(Integer waitTimeSeconds) {
      this.waitTimeSeconds = waitTimeSeconds;
      return this;
    }

    public Builder withMaxMessages(Integer maxMessages) {
      this.maxMessages = maxMessages;
      return this;
    }

    public SqsMessageReader build() {
      checkNotNull(queueUrl);

      consumer = (consumer == null)
          ? defaultConsumer() : consumer;
      messageDeserializer = (messageDeserializer == null)
          ? defaultMessageDeserializer() : messageDeserializer;
      maxMessages = (maxMessages == null)
          ? DEFAULT_MAX_MESSAGES : maxMessages;
      waitTimeSeconds = (waitTimeSeconds == null)
          ? DEFAULT_POLLING_WAIT_TIME_SECONDS : waitTimeSeconds;

      return new SqsMessageReader(queueUrl, waitTimeSeconds, maxMessages, messageDeserializer, consumer);
    }

    private AmazonSQS defaultConsumer() {
      return AmazonSQSClientBuilder.standard().build();
    }

    private MessageDeserializer defaultMessageDeserializer() {
      ObjectMapper mapper = new ObjectMapper()
          .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
      JsonMetaStoreEventDeserializer delegateSerDe = new JsonMetaStoreEventDeserializer(mapper);
      return new DefaultSqsMessageDeserializer(delegateSerDe, mapper);
    }
  }
}
