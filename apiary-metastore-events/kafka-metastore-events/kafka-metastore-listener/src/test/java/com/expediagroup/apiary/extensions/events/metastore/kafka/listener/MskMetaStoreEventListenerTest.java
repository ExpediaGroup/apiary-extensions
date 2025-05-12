/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.events.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expediagroup.apiary.extensions.events.metastore.event.*;
import com.expediagroup.apiary.extensions.events.metastore.io.MetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessage;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.MskMessageSender;

@RunWith(MockitoJUnitRunner.class)
public class MskMetaStoreEventListenerTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final byte[] PAYLOAD = "payload".getBytes();

  private @Mock MetaStoreEventSerDe eventSerDe;
  private @Mock ApiaryListenerEventFactory apiaryListenerEventFactory;
  private @Mock MskMessageSender mskMessageSender;

  private final Configuration config = new Configuration();
  private MskMetaStoreEventListener listener;

  @Before
  public void init() {
    when(eventSerDe.marshal(any(ApiaryListenerEvent.class))).thenReturn(PAYLOAD);
    listener = new MskMetaStoreEventListener(config, apiaryListenerEventFactory, eventSerDe, mskMessageSender);
  }

  @Test
  public void onCreateTable() {
    CreateTableEvent event = mock(CreateTableEvent.class);
    ApiaryCreateTableEvent apiaryEvent = mock(ApiaryCreateTableEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onCreateTable(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAlterTable() {
    AlterTableEvent event = mock(AlterTableEvent.class);
    ApiaryAlterTableEvent apiaryEvent = mock(ApiaryAlterTableEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onAlterTable(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onDropTable() {
    DropTableEvent event = mock(DropTableEvent.class);
    ApiaryDropTableEvent apiaryEvent = mock(ApiaryDropTableEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onDropTable(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAddPartition() {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    ApiaryAddPartitionEvent apiaryEvent = mock(ApiaryAddPartitionEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onAddPartition(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onAlterPartition() {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    ApiaryAlterPartitionEvent apiaryEvent = mock(ApiaryAlterPartitionEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onAlterPartition(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onDropPartition() {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    ApiaryDropPartitionEvent apiaryEvent = mock(ApiaryDropPartitionEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onDropPartition(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onInsert() {
    InsertEvent event = mock(InsertEvent.class);
    ApiaryInsertEvent apiaryEvent = mock(ApiaryInsertEvent.class);
    when(apiaryEvent.getDatabaseName()).thenReturn(DATABASE);
    when(apiaryEvent.getTableName()).thenReturn(TABLE);
    when(apiaryListenerEventFactory.create(event)).thenReturn(apiaryEvent);
    listener.onInsert(event);
    verify(mskMessageSender).send(any(KafkaMessage.class));
  }

  @Test
  public void onConfigChange() {
    listener.onConfigChange(mock(ConfigChangeEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

  @Test
  public void onCreateDatabase() {
    listener.onCreateDatabase(mock(CreateDatabaseEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

  @Test
  public void onDropDatabase() {
    listener.onDropDatabase(mock(DropDatabaseEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

  @Test
  public void onLoadPartitionDone() {
    listener.onLoadPartitionDone(mock(LoadPartitionDoneEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

  @Test
  public void onCreateFunction() {
    listener.onCreateFunction(mock(CreateFunctionEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

  @Test
  public void onDropFunction() {
    listener.onDropFunction(mock(DropFunctionEvent.class));
    verify(mskMessageSender, never()).send(any(KafkaMessage.class));
    verify(eventSerDe, never()).marshal(any(ApiaryListenerEvent.class));
  }

}
