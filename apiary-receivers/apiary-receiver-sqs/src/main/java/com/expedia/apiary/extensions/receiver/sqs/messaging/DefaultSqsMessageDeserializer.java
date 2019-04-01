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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageDeserializer;
import com.expedia.apiary.extensions.receiver.common.messaging.MetaStoreEventDeserializer;
import com.expedia.apiary.extensions.receiver.sqs.model.SqsMessage;

public class DefaultSqsMessageDeserializer implements MessageDeserializer {
  private static final Logger log = LoggerFactory.getLogger(DefaultSqsMessageDeserializer.class);

  private final ObjectMapper mapper;
  private final MetaStoreEventDeserializer delegateSerDe;

  public DefaultSqsMessageDeserializer(MetaStoreEventDeserializer delegateSerDe, ObjectMapper objectMapper) {
    this.delegateSerDe = delegateSerDe;
    this.mapper = objectMapper;
  }

  public <T extends ListenerEvent> T unmarshal(String payload) throws SerDeException {
    try {
      log.debug("Unmarshalled payload is: {}", payload);
      SqsMessage sqsMessage = mapper
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
          .readerFor(SqsMessage.class)
          .readValue(payload);
      T event = delegateSerDe.unmarshal(sqsMessage.getMessage());
      return event;
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshal event from payload", e);
    }
  }
}
