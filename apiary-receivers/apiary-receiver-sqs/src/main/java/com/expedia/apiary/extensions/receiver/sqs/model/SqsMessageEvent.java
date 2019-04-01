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
package com.expedia.apiary.extensions.receiver.sqs.model;

import java.util.Map;

import com.expedia.apiary.extensions.receiver.common.event.ListenerEvent;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;
import com.expedia.apiary.extensions.receiver.common.messaging.MessageEvent;

public class SqsMessageEvent implements MessageEvent {

  private ListenerEvent event;
  private Map<MessageProperty, String> messageDetails;

  public SqsMessageEvent(ListenerEvent event, Map<MessageProperty, String> messageProperties) {
    this.event = event;
    this.messageDetails = messageProperties;
  }

  @Override
  public ListenerEvent getEvent() {
    return event;
  }

  @Override
  public Map<MessageProperty, String> getMessageProperties() {
    return messageDetails;
  }
}
