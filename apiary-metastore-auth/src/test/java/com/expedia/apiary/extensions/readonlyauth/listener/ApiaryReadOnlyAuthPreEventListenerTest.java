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
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryReadOnlyAuthPreEventListenerTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Mock
  private Configuration configuration;

  private String tableName = "some_table";
  private String databaseName = "some_table";

  private ApiaryReadOnlyAuthPreEventListener readOnlyAuth;

  @Before
  public void setup() throws HiveException {
    environmentVariables.set("SHARED_HIVE_DB_NAMES", "db1,db2");

    readOnlyAuth = new ApiaryReadOnlyAuthPreEventListener(configuration);
  }

  @Test(expected = InvalidOperationException.class)
  public void onReadTableEvent1() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreReadTableEvent context = mock(PreReadTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(databaseName);
    when(context.getTable()).thenReturn(table);

    readOnlyAuth.onEvent(context);
  }

  @Test
  public void onReadTableEvent2() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreReadTableEvent context = mock(PreReadTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName("db1");
    when(context.getTable()).thenReturn(table);

    readOnlyAuth.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onCreateTableEvent() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreCreateTableEvent context = mock(PreCreateTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.CREATE_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(databaseName);

    readOnlyAuth.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onReadDatabaseEvent1() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreReadDatabaseEvent context = mock(PreReadDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_DATABASE);

    Database db = new Database();
    db.setName(databaseName);
    when(context.getDatabase()).thenReturn(db);

    readOnlyAuth.onEvent(context);
  }

  @Test
  public void onReadDatabaseEvent2() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreReadDatabaseEvent context = mock(PreReadDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.READ_DATABASE);

    Database db = new Database();
    db.setName("db2");
    when(context.getDatabase()).thenReturn(db);

    readOnlyAuth.onEvent(context);
  }

  @Test(expected = InvalidOperationException.class)
  public void onCreateDatabaseEvent() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreCreateDatabaseEvent context = mock(PreCreateDatabaseEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.CREATE_DATABASE);

    Database db = new Database();
    db.setName(databaseName);

    readOnlyAuth.onEvent(context);
  }
}
