/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.listener;

import static com.expediagroup.apiary.extensions.events.metastore.KafkaProducerProperty.SERDE_CLASS;
import static com.expediagroup.apiary.extensions.events.metastore.common.PropertyUtils.stringProperty;
import static com.expediagroup.apiary.extensions.events.metastore.common.io.MetaStoreEventSerDe.serDeForClassName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.common.listener.AbstractMetaStoreEventListener;
import com.expediagroup.apiary.extensions.events.metastore.common.event.SerializableListenerEventFactory;
import com.expediagroup.apiary.extensions.events.metastore.common.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.common.messaging.MessageTaskFactory;
import com.expediagroup.apiary.extensions.events.metastore.messaging.KafkaMessageTaskFactory;

public class KafkaMetaStoreEventListener extends AbstractMetaStoreEventListener {

  private final MetaStoreEventSerDe eventSerDe;
  private final MessageTaskFactory messageTaskFactory;

  public KafkaMetaStoreEventListener(Configuration config) {
    this(config, new SerializableListenerEventFactory(config), serDeForClassName(stringProperty(config, SERDE_CLASS)),
        new KafkaMessageTaskFactory(config), Executors.newSingleThreadExecutor());
  }

  @VisibleForTesting
  KafkaMetaStoreEventListener(
      Configuration config,
      SerializableListenerEventFactory serializableListenerEventFactory,
      MetaStoreEventSerDe eventSerDe,
      MessageTaskFactory messageTaskFactory,
      ExecutorService executorService) {
    super(config, serializableListenerEventFactory, executorService);
    this.eventSerDe = eventSerDe;
    this.messageTaskFactory = messageTaskFactory;
  }

  @Override
  protected MetaStoreEventSerDe getMetaStoreEventSerDe() {
    return eventSerDe;
  }

  @Override
  protected MessageTaskFactory getMessageTaskFactory() {
    return messageTaskFactory;
  }

}
