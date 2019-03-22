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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.Optional;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import com.expedia.apiary.extensions.receiver.common.MessageReader;
import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageDeserializer;

public class SqsMessageReader implements MessageReader {
  private static final Integer DEFAULT_POLLING_WAIT_TIME = 10;
  private static final Integer DEFAULT_MAX_MESSAGES = 10;

  private String queueUrl;
  private int waitTimeSeconds;
  private int maxMessages;
  private SqsMessageDeserializer sqsMessageDeserializer;
  private AmazonSQS consumer;
  private Iterator<Message> records;

  private SqsMessageReader(String queueUrl, int waitTimeSeconds, int maxMessages,
                          SqsMessageDeserializer sqsMessageDeserializer, AmazonSQS consumer) {
    this.queueUrl = queueUrl;
    this.waitTimeSeconds = waitTimeSeconds;
    this.maxMessages = maxMessages;
    this.sqsMessageDeserializer = sqsMessageDeserializer;
    this.consumer = consumer;
  }

  @Override
  public void close() {
    consumer.shutdown();
  }

  @Override
  public Optional<ListenerEvent> next() {
    readRecords();
    if (records.hasNext()) {
      return Optional.ofNullable(eventPayLoad(records.next()));
    } else {
      return Optional.empty();
    }
  }

  public void delete(Message message) {
    DeleteMessageRequest request = new DeleteMessageRequest()
        .withQueueUrl(queueUrl)
        .withReceiptHandle(message.getReceiptHandle());
    consumer.deleteMessage(request);
  }

  private void readRecords() {
    if (records == null || !records.hasNext()) {
      ReceiveMessageRequest request = new ReceiveMessageRequest()
          .withQueueUrl(queueUrl)
          .withWaitTimeSeconds(waitTimeSeconds)
          .withMaxNumberOfMessages(maxMessages);
      records = consumer.receiveMessage(request).getMessages().iterator();
    }
  }

  private ListenerEvent eventPayLoad(Message message) {
    try {
      return sqsMessageDeserializer.unmarshal(message.getBody());
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }

  public static final class Builder {
    private String queueUrl;
    private Integer waitTimeSeconds;
    private Integer maxMessages;
    private AmazonSQS consumer;
    private SqsMessageDeserializer sqsMessageDeserializer;

    public Builder(String queueUrl, AmazonSQS consumer, SqsMessageDeserializer sqsMessageDeserializer) {
      this.queueUrl = queueUrl;
      this.consumer = consumer;
      this.sqsMessageDeserializer = sqsMessageDeserializer;
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
      checkNotNull(consumer);
      checkNotNull(sqsMessageDeserializer);

      maxMessages = maxMessages == null
          ? DEFAULT_MAX_MESSAGES : maxMessages;
      waitTimeSeconds = waitTimeSeconds == null
          ? DEFAULT_POLLING_WAIT_TIME : waitTimeSeconds;

      return new SqsMessageReader(queueUrl, waitTimeSeconds, maxMessages, sqsMessageDeserializer, consumer);
    }
  }
}
