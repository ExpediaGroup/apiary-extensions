/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener;

import static java.util.Arrays.asList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryGlueSyncTest {

  @Mock
  private AWSGlue glueClient;

  @Mock
  private Configuration configuration;
  @Mock
  private CreateTableResult createTableResult;

  @Captor
  private ArgumentCaptor<CreateTableRequest> requestCaptor;

  private final String tableName = "some_table";
  private final String dbName = "some_db";
  private final String[] colNames = { "col1", "col2", "col3" };
  private final String[] partNames = { "part1", "part2" };

  private final String gluePrefix = "test_";
  private ApiaryGlueSync glueSync;

  @Before
  public void setup() {
    glueSync = new ApiaryGlueSync(configuration, glueClient, gluePrefix);
    when(glueClient.createTable(any(CreateTableRequest.class))).thenReturn(createTableResult);
  }

  @Test
  public void onCreateTable() throws MetaException {
    CreateTableEvent event = mock(CreateTableEvent.class);
    when(event.getStatus()).thenReturn(true);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(dbName);

    StorageDescriptor sd = new StorageDescriptor();

    List<FieldSchema> fields = new ArrayList<>();
    for (String colName : colNames) {
      fields.add(new FieldSchema(colName, "string", ""));
    }
    sd.setCols(fields);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    table.setSd(sd);

    List<FieldSchema> partitions = new ArrayList<>();
    for (String partName : partNames) {
      partitions.add(new FieldSchema(partName, "string", ""));
    }
    table.setPartitionKeys(partitions);

    when(event.getTable()).thenReturn(table);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(requestCaptor.capture());
    CreateTableRequest createTableRequest = requestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(), is(gluePrefix + dbName));
    assertThat(createTableRequest.getTableInput().getName(), is(tableName));
    assertThat(toList(createTableRequest.getTableInput().getPartitionKeys()), is(asList(partNames)));
    assertThat(toList(createTableRequest.getTableInput().getStorageDescriptor().getColumns()), is(asList(colNames)));
  }

  private List<String> toList(List<Column> columns) {
    return columns.stream().map(p -> p.getName()).collect(Collectors.toList());
  }

  @Test
  public void onCreateUnpartitionedTable() throws MetaException {
    CreateTableEvent event = mock(CreateTableEvent.class);
    when(event.getStatus()).thenReturn(true);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(dbName);

    StorageDescriptor sd = new StorageDescriptor();

    List<FieldSchema> fields = new ArrayList<>();
    for (String colName : colNames) {
      fields.add(new FieldSchema(colName, "string", ""));
    }
    sd.setCols(fields);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    table.setSd(sd);
    when(event.getTable()).thenReturn(table);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(requestCaptor.capture());
    CreateTableRequest createTableRequest = requestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(), is(gluePrefix + dbName));
    assertThat(createTableRequest.getTableInput().getName(), is(tableName));
    assertThat(createTableRequest.getTableInput().getPartitionKeys().size(), is(0));
    assertThat(toList(createTableRequest.getTableInput().getStorageDescriptor().getColumns()), is(asList(colNames)));
  }

}
