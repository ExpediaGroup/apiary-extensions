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
import static com.expediagroup.apiary.extensions.events.metastore.listener.ListenerUtils.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.ConfigChangeEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.LoadPartitionDoneEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.common.event.SerializableListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.common.event.SerializableListenerEventFactory;
import com.expediagroup.apiary.extensions.events.metastore.common.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.messaging.KafkaMessage;
import com.expediagroup.apiary.extensions.events.metastore.messaging.KafkaMessageSender;

public class KafkaMetaStoreEventListener extends MetaStoreEventListener {
  private static final Logger log = LoggerFactory.getLogger(KafkaMetaStoreEventListener.class);

  private final MetaStoreEventSerDe eventSerDe;
  private final KafkaMessageSender kafkaMessageSender;
  private final SerializableListenerEventFactory serializableListenerEventFactory;

  public KafkaMetaStoreEventListener(Configuration config) {
    this(config, new SerializableListenerEventFactory(config), serDeForClassName(stringProperty(config, SERDE_CLASS)),
        new KafkaMessageSender(config));
  }

  @VisibleForTesting
  KafkaMetaStoreEventListener(
      Configuration config,
      SerializableListenerEventFactory serializableListenerEventFactory,
      MetaStoreEventSerDe eventSerDe,
    KafkaMessageSender kafkaMessageSender) {
    super(config);
    this.eventSerDe = eventSerDe;
    this.serializableListenerEventFactory = serializableListenerEventFactory;
    this.kafkaMessageSender = kafkaMessageSender;
  }

  private KafkaMessage withPayload(SerializableListenerEvent event) {
    return KafkaMessage
      .builder()
      .database(event.getDatabaseName())
      .table(event.getTableName())
      .payload(eventSerDe.marshal(event))
      .build();
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) {
    log.debug("Create table event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(tableEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onDropTable(DropTableEvent tableEvent) {
    log.debug("Drop table event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(tableEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent tableEvent) {
    log.debug("Alter table event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(tableEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent) {
    log.debug("Add partition event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(partitionEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent) {
    log.debug("Drop partition event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(partitionEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent partitionEvent) {
    log.debug("Alter partition event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(partitionEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onInsert(InsertEvent insertEvent) {
    log.debug("Insert event received");
    try {
      kafkaMessageSender.send(withPayload(serializableListenerEventFactory.create(insertEvent)));
    } catch (Exception e) {
      error(e);
    }
  }

  @Override
  public void onConfigChange(ConfigChangeEvent tableEvent) {}

  @Override
  public void onCreateDatabase(CreateDatabaseEvent dbEvent) {}

  @Override
  public void onDropDatabase(DropDatabaseEvent dbEvent) {}

  @Override
  public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) {}

  @Override
  public void onAddIndex(AddIndexEvent indexEvent) {}

  @Override
  public void onDropIndex(DropIndexEvent indexEvent) {}

  @Override
  public void onAlterIndex(AlterIndexEvent indexEvent) {}

  @Override
  public void onCreateFunction(CreateFunctionEvent fnEvent) {}

  @Override
  public void onDropFunction(DropFunctionEvent fnEvent) {}

}
