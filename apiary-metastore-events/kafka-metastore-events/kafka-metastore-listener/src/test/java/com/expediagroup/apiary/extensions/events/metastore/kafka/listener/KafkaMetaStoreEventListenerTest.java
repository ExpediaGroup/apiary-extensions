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
package com.expediagroup.apiary.extensions.events.metastore.kafka.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAddPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAlterPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAlterTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableDropPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableDropTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableInsertEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableListenerEventFactory;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessage;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageSender;

@RunWith(MockitoJUnitRunner.class)
public class KafkaMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock MetaStoreEventSerDe eventSerDe;
  private @Mock SerializableListenerEventFactory serializableListenerEventFactory;
  private @Mock KafkaMessageSender kafkaMessageSender;

  private final Configuration config = new Configuration();
  private KafkaMetaStoreEventListener listener;

  @Before
  public void init() {
    when(eventSerDe.marshal(any(SerializableListenerEvent.class))).thenReturn(PAYLOAD);
    listener = new KafkaMetaStoreEventListener(config, serializableListenerEventFactory, eventSerDe, kafkaMessageSender);
  }

  @Test
  public void onCreateTable() {
    CreateTableEvent event = mock(CreateTableEvent.class);
    SerializableCreateTableEvent serializableEvent = mock(SerializableCreateTableEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onCreateTable(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAlterTable() {
    AlterTableEvent event = mock(AlterTableEvent.class);
    SerializableAlterTableEvent serializableEvent = mock(SerializableAlterTableEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onAlterTable(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onDropTable() {
    DropTableEvent event = mock(DropTableEvent.class);
    SerializableDropTableEvent serializableEvent = mock(SerializableDropTableEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onDropTable(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAddPartition() {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    SerializableAddPartitionEvent serializableEvent = mock(SerializableAddPartitionEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onAddPartition(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAlterPartition() {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    SerializableAlterPartitionEvent serializableEvent = mock(SerializableAlterPartitionEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onAlterPartition(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onDropPartition() {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    SerializableDropPartitionEvent serializableEvent = mock(SerializableDropPartitionEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onDropPartition(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onInsert() {
    InsertEvent event = mock(InsertEvent.class);
    SerializableInsertEvent serializableEvent = mock(SerializableInsertEvent.class);
    when(serializableEvent.getDatabaseName()).thenReturn(DATABASE);
    when(serializableEvent.getTableName()).thenReturn(TABLE);
    when(serializableListenerEventFactory.create(event)).thenReturn(serializableEvent);
    listener.onInsert(event);
    verify(kafkaMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onConfigChange() {
    listener.onConfigChange(mock(ConfigChangeEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onCreateDatabase() {
    listener.onCreateDatabase(mock(CreateDatabaseEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropDatabase() {
    listener.onDropDatabase(mock(DropDatabaseEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onLoadPartitionDone() {
    listener.onLoadPartitionDone(mock(LoadPartitionDoneEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onAddIndex() {
    listener.onAddIndex(mock(AddIndexEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropIndex() {
    listener.onDropIndex(mock(DropIndexEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onAlterIndex() {
    listener.onAlterIndex(mock(AlterIndexEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onCreateFunction() {
    listener.onCreateFunction(mock(CreateFunctionEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

  @Test
  public void onDropFunction() {
    listener.onDropFunction(mock(DropFunctionEvent.class));
    verify(kafkaMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(SerializableListenerEvent.class));
  }

}
