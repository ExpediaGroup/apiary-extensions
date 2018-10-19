/**
 * Copyright (C) 2018 Expedia Inc.
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
package com.expedia.apiary.extensions.metastore.listener;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expedia.apiary.extensions.metastore.listener.ApiarySnsListener.PROTOCOL_VERSION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ApiarySnsListenerTest {

  @Mock
  private AmazonSNS snsClient;
  @Mock
  private Configuration configuration;
  @Mock
  private PublishResult publishResult;

  @Captor
  private ArgumentCaptor<PublishRequest> requestCaptor;

  private final Table table = new Table();
  private static final String TABLE_NAME = "some_table";
  private static final String DB_NAME = "some_db";
  private static final List<String> PARTITION_VALUES = ImmutableList.of("value_1", "1000", "value_2");
  private static final List<String> NEW_PARTITION_VALUES = ImmutableList.of("value_3", "2000", "value_4");

  private List<FieldSchema> partitionKeys;
  private ApiarySnsListener snsListener;

  @Before
  public void setup() {
    snsListener = new ApiarySnsListener(configuration, snsClient);
    when(snsClient.publish(any(PublishRequest.class))).thenReturn(publishResult);

    FieldSchema partitionColumn1 = new FieldSchema("column_1", "string", "");
    FieldSchema partitionColumn2 = new FieldSchema("column_2", "int", "");
    FieldSchema partitionColumn3 = new FieldSchema("column_3", "string", "");

    partitionKeys = ImmutableList.of(partitionColumn1, partitionColumn2, partitionColumn3);

    table.setTableName(TABLE_NAME);
    table.setDbName(DB_NAME);
    table.setPartitionKeys(partitionKeys);
  }

  @Test
  public void onCreateTable() throws MetaException {
    CreateTableEvent event = mock(CreateTableEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(table);

    snsListener.onCreateTable(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();
    assertThat(publishRequest.getMessage(),
        is("{\"protocolVersion\":\""
            + PROTOCOL_VERSION
            + "\",\"eventType\":\"CREATE_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\"}"));
  }

  @Test
  public void onInsert() throws MetaException {
    InsertEvent event = mock(InsertEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(TABLE_NAME);
    when(event.getDb()).thenReturn(DB_NAME);
    List<String> files = Arrays.asList("file:/a/b.txt", "file:/a/c.txt");
    when(event.getFiles()).thenReturn(files);
    List<String> fileChecksums = Arrays.asList("123", "456");
    when(event.getFileChecksums()).thenReturn(fileChecksums);

    Map<String, String> partitionKeyValues = new HashMap<>();
    partitionKeyValues.put("load_date", "2013-03-24");
    partitionKeyValues.put("variant_code", "EN");
    when(event.getPartitionKeyValues()).thenReturn(partitionKeyValues);

    snsListener.onInsert(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();
    assertThat(publishRequest.getMessage(), is("{\"protocolVersion\":\""
        + PROTOCOL_VERSION
        + "\",\"eventType\":\"INSERT\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"files\":[\"file:/a/b.txt\",\"file:/a/c.txt\"],"
        + "\"fileChecksums\":[\"123\",\"456\"],\"partitionKeyValues\":{\"load_date\":\"2013-03-24\",\"variant_code\":\"EN\"}}"));
  }

  @Test
  public void onAddPartition() throws MetaException {
    AddPartitionEvent event = mock(AddPartitionEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(table);

    List<Partition> partitions = new ArrayList<>();
    partitions
        .add(new Partition(PARTITION_VALUES, DB_NAME, TABLE_NAME, 0, 0, createStorageDescriptor(partitionKeys),
            ImmutableMap.of()));

    when(event.getPartitionIterator()).thenReturn(partitions.iterator());

    snsListener.onAddPartition(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();

    assertThat(publishRequest.getMessage(), is("{\"protocolVersion\":\""
        + PROTOCOL_VERSION
        + "\",\"eventType\":\"ADD_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\":{\"column_1\":\"string\",\"column_2\":\"int\",\"column_3\":\"string\"},\"partitionValues\":[\"value_1\",\"1000\",\"value_2\"]}"));
  }

  @Test
  public void onDropPartition() throws MetaException {
    DropPartitionEvent event = mock(DropPartitionEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(table);

    List<Partition> partitions = new ArrayList<>();
    partitions
        .add(new Partition(PARTITION_VALUES, DB_NAME, TABLE_NAME, 0, 0, createStorageDescriptor(partitionKeys),
            ImmutableMap.of()));

    when(event.getPartitionIterator()).thenReturn(partitions.iterator());

    snsListener.onDropPartition(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();

    assertThat(publishRequest.getMessage(), is("{\"protocolVersion\":\""
        + PROTOCOL_VERSION
        + "\",\"eventType\":\"DROP_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\":{\"column_1\":\"string\",\"column_2\":\"int\",\"column_3\":\"string\"},\"partitionValues\":[\"value_1\",\"1000\",\"value_2\"]}"));
  }

  @Test
  public void onDropTable() throws MetaException {
    DropTableEvent event = mock(DropTableEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(table);

    snsListener.onDropTable(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();

    assertThat(publishRequest.getMessage(),
        is("{\"protocolVersion\":\""
            + PROTOCOL_VERSION
            + "\",\"eventType\":\"DROP_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\"}"));
  }

  @Test
  public void onAlterPartition() throws MetaException {
    AlterPartitionEvent event = mock(AlterPartitionEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getTable()).thenReturn(table);

    Partition oldPartition = new Partition(PARTITION_VALUES, DB_NAME, TABLE_NAME, 0, 0,
        createStorageDescriptor(partitionKeys), ImmutableMap.of());
    Partition newPartition = new Partition(NEW_PARTITION_VALUES, DB_NAME, TABLE_NAME, 0, 0,
        createStorageDescriptor(partitionKeys), ImmutableMap.of());

    when(event.getOldPartition()).thenReturn(oldPartition);
    when(event.getNewPartition()).thenReturn(newPartition);

    snsListener.onAlterPartition(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();

    assertThat(publishRequest.getMessage(), is("{\"protocolVersion\":\""
        + PROTOCOL_VERSION
        + "\",\"eventType\":\"ALTER_PARTITION\",\"dbName\":\"some_db\",\"tableName\":\"some_table\",\"partitionKeys\":{\"column_1\":\"string\",\"column_2\":\"int\",\"column_3\":\"string\"},\"partitionValues\":[\"value_3\",\"2000\",\"value_4\"],\"oldPartitionValues\":[\"value_1\",\"1000\",\"value_2\"]}"));
  }

  @Test
  public void onAlterTable() throws MetaException {
    Table newTable = new Table();
    newTable.setTableName("new_" + TABLE_NAME);
    newTable.setDbName(DB_NAME);

    AlterTableEvent event = mock(AlterTableEvent.class);
    when(event.getStatus()).thenReturn(true);
    when(event.getOldTable()).thenReturn(table);
    when(event.getNewTable()).thenReturn(newTable);

    snsListener.onAlterTable(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();

    assertThat(publishRequest.getMessage(), is("{\"protocolVersion\":\""
        + PROTOCOL_VERSION
        + "\",\"eventType\":\"ALTER_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"new_some_table\",\"oldTableName\":\"some_table\"}"));
  }

  private StorageDescriptor createStorageDescriptor(List<FieldSchema> partitionKeys) {
    return new StorageDescriptor(partitionKeys, "s3://test_location", "ORC", "ORC", false, 2, new SerDeInfo(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyMap());
  }
  // TODO: test for setting ARN via environment variable
}
