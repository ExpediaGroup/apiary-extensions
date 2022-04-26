/**
 * Copyright (C) 2018-2022 Expedia, Inc.
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
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryGlueSyncTest {

  @Mock
  private AWSGlue glueClient;

  @Mock
  private Configuration configuration;
  @Mock
  private CreateTableResult createTableResult;

  @Captor
  private ArgumentCaptor<CreateTableRequest> tableRequestCaptor;
  @Captor
  private ArgumentCaptor<CreateDatabaseRequest> createDatabaseRequestCaptor;
  @Captor
  private ArgumentCaptor<UpdateDatabaseRequest> updateDatabaseRequestCaptor;
  @Captor
  private ArgumentCaptor<DeleteDatabaseRequest> deleteDatabaseRequestCaptor;

  private final String tableName = "some_table";
  private final String dbName = "some_db";
  private final String[] colNames = { "col1", "col2", "col3" };
  private final String[] partNames = { "part1", "part2" };
  private final String locationUri = "uri";
  private final String description = "desc";
  private final ImmutableMap<String, String> params = ImmutableMap.of("created-by", "apiary-glue-sync");

  private final String gluePrefix = "test_";
  private ApiaryGlueSync glueSync;

  @Before
  public void setup() {
    glueSync = new ApiaryGlueSync(configuration, glueClient, gluePrefix);
    when(glueClient.createTable(any(CreateTableRequest.class))).thenReturn(createTableResult);
  }

  @Test
  public void onCreateDatabase() {
    CreateDatabaseEvent event = mock(CreateDatabaseEvent.class);
    when(event.getStatus()).thenReturn(true);

    Database database = getDatabase(description, locationUri, params);
    when(event.getDatabase()).thenReturn(database);

    glueSync.onCreateDatabase(event);

    verify(glueClient).createDatabase(createDatabaseRequestCaptor.capture());
    CreateDatabaseRequest createDatabaseRequest = createDatabaseRequestCaptor.getValue();

    assertThat(createDatabaseRequest.getDatabaseInput().getName(), is(gluePrefix + dbName));
    assertThat(createDatabaseRequest.getDatabaseInput().getLocationUri(), is(locationUri));
    assertThat(createDatabaseRequest.getDatabaseInput().getParameters(), is(params));
    assertThat(createDatabaseRequest.getDatabaseInput().getDescription(), is(description));
  }

  @Test
  public void onCreateDatabaseThatAlreadyExists() {
    CreateDatabaseEvent event = mock(CreateDatabaseEvent.class);
    when(event.getStatus()).thenReturn(true);

    Database database = getDatabase(description, locationUri, params);
    when(event.getDatabase()).thenReturn(database);
    when(glueClient.createDatabase(any())).thenThrow(new AlreadyExistsException(""));

    glueSync.onCreateDatabase(event);

    verify(glueClient).createDatabase(createDatabaseRequestCaptor.capture());
    verify(glueClient).updateDatabase(updateDatabaseRequestCaptor.capture());
    UpdateDatabaseRequest updateDatabaseRequest = updateDatabaseRequestCaptor.getValue();

    assertThat(updateDatabaseRequest.getName(), is(gluePrefix + dbName));
    assertThat(updateDatabaseRequest.getDatabaseInput().getName(), is(gluePrefix + dbName));
    assertThat(updateDatabaseRequest.getDatabaseInput().getLocationUri(), is(locationUri));
    assertThat(updateDatabaseRequest.getDatabaseInput().getParameters(), is(params));
    assertThat(updateDatabaseRequest.getDatabaseInput().getDescription(), is(description));
  }

  @Test
  public void onDropDatabase() {
    DropDatabaseEvent event = mock(DropDatabaseEvent.class);
    when(event.getStatus()).thenReturn(true);

    Database database = getDatabase(description, locationUri, params);
    when(event.getDatabase()).thenReturn(database);

    glueSync.onDropDatabase(event);

    verify(glueClient).deleteDatabase(deleteDatabaseRequestCaptor.capture());
    DeleteDatabaseRequest deleteDatabaseRequest = deleteDatabaseRequestCaptor.getValue();
    assertThat(deleteDatabaseRequest.getName(), is(gluePrefix + dbName));
  }

  @Test
  public void onDropDatabaseThatDoesntExist() {
    DropDatabaseEvent event = mock(DropDatabaseEvent.class);
    when(event.getStatus()).thenReturn(true);

    Database database = getDatabase(description, locationUri, params);
    when(event.getDatabase()).thenReturn(database);
    when(glueClient.deleteDatabase(any())).thenThrow(new EntityNotFoundException(""));

    glueSync.onDropDatabase(event);

    verify(glueClient).deleteDatabase(deleteDatabaseRequestCaptor.capture());
    verifyNoMoreInteractions(glueClient);
  }

  @Test
  public void onDropDatabaseNotCreatedByGlueSync() {
    DropDatabaseEvent event = mock(DropDatabaseEvent.class);
    when(event.getStatus()).thenReturn(true);

    Database database = getDatabase(description, locationUri, Collections.emptyMap());
    when(event.getDatabase()).thenReturn(database);

    glueSync.onDropDatabase(event);

    verifyZeroInteractions(glueClient);
  }

  @Test
  public void onCreateTable() {
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
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<>());
    table.setSd(sd);

    List<FieldSchema> partitions = new ArrayList<>();
    for (String partName : partNames) {
      partitions.add(new FieldSchema(partName, "string", ""));
    }
    table.setPartitionKeys(partitions);

    when(event.getTable()).thenReturn(table);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(tableRequestCaptor.capture());
    CreateTableRequest createTableRequest = tableRequestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(), is(gluePrefix + dbName));
    assertThat(createTableRequest.getTableInput().getName(), is(tableName));
    assertThat(toList(createTableRequest.getTableInput().getPartitionKeys()), is(asList(partNames)));
    assertThat(toList(createTableRequest.getTableInput().getStorageDescriptor().getColumns()), is(asList(colNames)));
  }

  @Test
  public void onCreateUnpartitionedTable() {
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
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<>());
    table.setSd(sd);
    when(event.getTable()).thenReturn(table);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(tableRequestCaptor.capture());
    CreateTableRequest createTableRequest = tableRequestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(), is(gluePrefix + dbName));
    assertThat(createTableRequest.getTableInput().getName(), is(tableName));
    assertThat(createTableRequest.getTableInput().getPartitionKeys().size(), is(0));
    assertThat(toList(createTableRequest.getTableInput().getStorageDescriptor().getColumns()), is(asList(colNames)));
  }

  private List<String> toList(List<Column> columns) {
    return columns.stream().map(Column::getName).collect(Collectors.toList());
  }

  private Database getDatabase(String description, String locationUri, Map<String, String> params) {
    Database database = new Database();
    database.setName(dbName);
    database.setDescription(description);
    database.setLocationUri(locationUri);
    database.setParameters(params);
    return database;
  }
}
