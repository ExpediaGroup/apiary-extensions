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
package com.expedia.apiary.extensions.readonlyauth.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryReadOnlyAuthPreEventListenerTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Mock
  private Configuration configuration;

  private String tableName = "some_table";
  private String unauthorizedDatabaseName = "some_db";
  private String authorizedDatabaseName1 = "db1";
  private String authorizedDatabaseName2 = "db2";

  private ApiaryReadOnlyAuthPreEventListener listener;

  @Before
  public void setup() throws HiveException {
    environmentVariables.set("SHARED_HIVE_DB_NAMES",
        authorizedDatabaseName1 + "," + authorizedDatabaseName2);

    listener = new ApiaryReadOnlyAuthPreEventListener(configuration);
  }

  @Test
  public void onReadTableEventAuthorized() throws Exception {
    PreReadTableEvent context = mock(PreReadTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(authorizedDatabaseName1);
    when(context.getTable()).thenReturn(table);

    listener.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onReadTableEventUnauthorized() throws Exception {
    PreReadTableEvent context = mock(PreReadTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(unauthorizedDatabaseName);
    when(context.getTable()).thenReturn(table);

    listener.onEvent(context);
  }

  @Test
  public void onReadDatabaseEventAuthorized() throws Exception {
    PreReadDatabaseEvent context = mock(PreReadDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_DATABASE);

    Database db = new Database();
    db.setName(authorizedDatabaseName2);
    when(context.getDatabase()).thenReturn(db);

    listener.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onReadDatabaseEventUnauthorized() throws Exception {
    PreReadDatabaseEvent context = mock(PreReadDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_DATABASE);

    Database db = new Database();
    db.setName(unauthorizedDatabaseName);
    when(context.getDatabase()).thenReturn(db);

    listener.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onCreateTableEvent() throws Exception {
    PreCreateTableEvent context = mock(PreCreateTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.CREATE_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(authorizedDatabaseName1);

    listener.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onCreateDatabaseEvent() throws Exception {
    PreCreateDatabaseEvent context = mock(PreCreateDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.CREATE_DATABASE);

    Database db = new Database();
    db.setName(authorizedDatabaseName1);

    listener.onEvent(context);
  }

  @Test
  public void dbListTrimmed() throws Exception {
    environmentVariables.set("SHARED_HIVE_DB_NAMES",
        authorizedDatabaseName1 + "  ,   " + authorizedDatabaseName2);
    listener = new ApiaryReadOnlyAuthPreEventListener(configuration);

    PreReadDatabaseEvent context = mock(PreReadDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_DATABASE);
    Database db = new Database();
    db.setName(authorizedDatabaseName2);
    when(context.getDatabase()).thenReturn(db);
    listener.onEvent(context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullDbList() throws HiveException {
    environmentVariables.set("SHARED_HIVE_DB_NAMES", null);
    listener = new ApiaryReadOnlyAuthPreEventListener(configuration);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyDbList() throws HiveException {
    environmentVariables.set("SHARED_HIVE_DB_NAMES", "");
    listener = new ApiaryReadOnlyAuthPreEventListener(configuration);
  }
}
