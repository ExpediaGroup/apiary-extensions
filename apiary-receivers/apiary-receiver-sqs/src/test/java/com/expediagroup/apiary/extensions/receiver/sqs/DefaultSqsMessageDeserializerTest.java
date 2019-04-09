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
package com.expediagroup.apiary.extensions.receiver.sqs;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.expediagroup.apiary.extensions.receiver.common.error.ApiaryReceiverException;
import com.expediagroup.apiary.extensions.receiver.common.messaging.MetaStoreEventDeserializer;
import com.expediagroup.apiary.extensions.receiver.sqs.messaging.DefaultSqsMessageDeserializer;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSqsMessageDeserializerTest {
  private @Mock MetaStoreEventDeserializer delegateSerDe;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final String BASE_EVENT_FROM_SNS = "{"
      + "  \"Type\" : \"Notification\","
      + "  \"MessageId\" : \"message-id\","
      + "  \"TopicArn\" : \"arn:aws:sns:us-west-2:sns-topic\","
      + "  \"Timestamp\" : \"2018-10-23T13:01:54.507Z\","
      + "  \"SignatureVersion\" : \"1\","
      + "  \"Signature\" : \"signature\","
      + "  \"SigningCertURL\" : \"https://sns.us-west-2.amazonaws.com/SimpleNotificationService-xxxx\","
      + "  \"UnsubscribeURL\" : \"https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:440407435941:sns-topic\",";

  private DefaultSqsMessageDeserializer sqsMessageDeserializer;

  @Before
  public void init() {
    sqsMessageDeserializer = new DefaultSqsMessageDeserializer(delegateSerDe, OBJECT_MAPPER);
  }

  @Test
  public void typicalUnmarshal() {
    String message = getSqsMessage();
    String payload = "\"Message\" : \"" + message.replace("\"", "\\\"") + "\"";
    sqsMessageDeserializer.unmarshal(getSnsMessage(payload));
    verify(delegateSerDe).unmarshal(message);
  }

  @Test(expected = ApiaryReceiverException.class)
  public void exceptionUnmarshal() {
    String message = getSqsMessage();
    String payload = "\"Message\" : \"" + message.replace("\"", "\\\"") + "\"";
    when(delegateSerDe.unmarshal(any(String.class))).thenThrow(new ApiaryReceiverException("exception"));
    sqsMessageDeserializer.unmarshal(getSnsMessage(payload));
  }

  private String getSnsMessage(String eventMessage) {
    return BASE_EVENT_FROM_SNS + eventMessage + "}";
  }

  private String getSqsMessage() {
    return "{\"protocolVersion\":\"1.0\","
        + "\"eventType\":\"ADD_PARTITION\","
        + "\"dbName\":\"some_db\","
        + "\"tableName\":\"some_table\","
        + "\"tableLocation\":\"s3://table_location\","
        + "\"partitionKeys\":{\"col_1\":\"string\", \"col_2\": \"integer\", \"col_3\":\"string\"},"
        + "\"partitionValues\":[\"val_1\", \"val_2\", \"val_3\"],"
        + "\"partitionLocation\":\"s3://table_location/partition_location\","
        + "\"tableParameters\":{"
        + "\"param_1\": \"val_1\","
        + "\"param_2\": \"val_2\""
        + "}}";
  }
}
