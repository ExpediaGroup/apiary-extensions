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

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.annotations.VisibleForTesting;

import com.expedia.apiary.extensions.receiver.common.MessageReader;
import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.sqs.messaging.SqsMessageDeserializer;

public class SqsMessageReader implements MessageReader {
  private static final Integer DEFAULT_POLLING_WAIT_TIME = 10;
  private static final Integer DEFAULT_MAX_MESSAGES = 10;

  private final String queueUrl;
  private final SqsMessageDeserializer sqsDeserializer;
  private final int waitTimeSeconds;
  private final int maxMessages;
  private final AmazonSQS consumer;
  private Iterator<Message> records;

  public SqsMessageReader(SqsProperties properties, SqsMessageDeserializer sqsDeserializer) {
    this(properties, sqsDeserializer, getSqsConsumer(properties));
  }

  @VisibleForTesting
  SqsMessageReader(SqsProperties properties, SqsMessageDeserializer sqsDeserializer, AmazonSQS consumer) {
    this.queueUrl = checkNotNull(properties.getQueue());
    this.sqsDeserializer = sqsDeserializer;
    this.waitTimeSeconds = properties.getWaitTimeSeconds() == null
        ? DEFAULT_POLLING_WAIT_TIME : properties.getWaitTimeSeconds();
    this.maxMessages = properties.getMaxMessages() == null
        ? DEFAULT_MAX_MESSAGES : properties.getMaxMessages();
    this.consumer = consumer;
  }

  private static AmazonSQS getSqsConsumer(SqsProperties properties) {
    String awsAccessKey = checkNotNull(properties.getAwsAccessKey());
    String awsSecretKey = checkNotNull(properties.getAwsAccessKey());
    Regions region = properties.getRegion() == null
        ? Regions.DEFAULT_REGION : properties.getRegion();

    AWSCredentialsProviderChain credentials = getAwsCredentialsProviderChain(awsAccessKey, awsSecretKey);

    return AmazonSQSClientBuilder
        .standard()
        .withRegion(checkNotNull(region))
        .withCredentials(credentials)
        .build();
  }

  private static AWSCredentialsProviderChain getAwsCredentialsProviderChain(String awsAccessKey, String awsSecretKey) {
    return new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(),
        new InstanceProfileCredentialsProvider(false), new EC2ContainerCredentialsProviderWrapper(),
        new AWSStaticCredentialsProvider(new BasicAWSCredentials(checkNotNull(awsAccessKey),
            checkNotNull(awsSecretKey))));
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
      return sqsDeserializer.unmarshal(message.getBody());
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshall event", e);
    }
  }
}
