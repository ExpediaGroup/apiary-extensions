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
package com.expedia.apiary.extensions.receiver.common.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.expedia.apiary.extensions.receiver.common.error.SerDeException;
import com.expedia.apiary.extensions.receiver.common.event.EventType;
import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;

public class JsonMetaStoreEventDeserializer implements MetaStoreEventDeserializer {
  private static final Logger log = LoggerFactory.getLogger(JsonMetaStoreEventDeserializer.class);

  private final ObjectMapper mapper;

  public JsonMetaStoreEventDeserializer(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public <T extends ListenerEvent> T unmarshal(String payload) throws SerDeException {
    try {
      log.debug("Marshalled event is: {}", payload);

      // As we don't know the type in advance we can only deserialize the event twice:
      // 1. Create a dummy object just to find out the type
      T genericEvent = mapper.readerFor(HelperSerializableListenerEvent.class).readValue(payload);
      log.debug("Unmarshal event of type: {}", genericEvent.getEventType());
      // 2. Deserialize the actual object
      T event = mapper.readerFor(genericEvent.getEventType().eventClass()).readValue(payload);
      log.debug("Unmarshalled event is: {}", event);
      return event;
    } catch (Exception e) {
      throw new SerDeException("Unable to unmarshal event from payload", e);
    }
  }

  static class HelperSerializableListenerEvent extends ListenerEvent {
    private static final long serialVersionUID = 1L;

    private EventType eventType;

    HelperSerializableListenerEvent() {}

    @Override
    public EventType getEventType() {
      return eventType;
    }

    public void setEventType(EventType eventType) {
      this.eventType = eventType;
    }

    @Override
    public String getDbName() {
      return null;
    }

    @Override
    public String getTableName() {
      return null;
    }
  }
}
