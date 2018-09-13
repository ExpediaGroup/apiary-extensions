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
package com.expedia.apiary.extensions.gluesync.listener;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;

import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

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

  private String tableName = "some_table";
  private String dbName = "some_db";
  private String[] colNames = { "col1", "col2", "col3" };
  private String[] partNames = { "part1", "part2" };
  
  private String gluePrefix = "test_";
  private ApiaryGlueSync glueSync;

  @Before
  public void setup() {
    glueSync = new ApiaryGlueSync(configuration, glueClient,gluePrefix);
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

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (int i = 0; i < colNames.length; ++i) {
        fields.add(new FieldSchema(colNames[i], "string", ""));
    }
    sd.setCols(fields);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters().put(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    table.setSd(sd);

    List<FieldSchema> partitions = new ArrayList<FieldSchema>();
    for (int i = 0; i < partNames.length; ++i) {
        partitions.add(new FieldSchema(partNames[i], "string", ""));
    }
    table.setPartitionKeys(partitions);

    when(event.getTable()).thenReturn(table);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(requestCaptor.capture());
    CreateTableRequest createTableRequest = requestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(),is(gluePrefix+dbName));
    assertThat(createTableRequest.getTableInput().getName(),is(tableName));
    assertThat(createTableRequest.getTableInput().getPartitionKeys().stream().map(p -> p.getName()).collect(Collectors.toList()),
        is(partitions.stream().map(p -> p.getName()).collect(Collectors.toList())));
    assertThat(createTableRequest.getTableInput().getStorageDescriptor().getColumns().stream().map(p -> p.getName()).collect(Collectors.toList()),
        is(fields.stream().map(p -> p.getName()).collect(Collectors.toList())));
  }

  // TODO: tests for other onXxx() methods
}
