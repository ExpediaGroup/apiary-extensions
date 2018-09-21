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
package com.expedia.apiary.extensions.rangerauth.listener;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext.PreEventType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryRangerAuthPreEventListenerTest {

  private RangerBasePlugin plugin;

  @Mock
  private Configuration configuration;

  private String tableName = "some_table";
  private String dbName = "rangerauthz";
  private String userName = "bob";

  private ApiaryRangerAuthPreEventListener rangerAuth;

  @Before
  public void setup() throws HiveException {
    plugin = new RangerBasePlugin("hive", "metastore");
    plugin.init();
    rangerAuth = new ApiaryRangerAuthPreEventListener(configuration, plugin);
  }

  @Test
  public void onEvent() throws MetaException, NoSuchObjectException, InvalidOperationException {
    PreCreateTableEvent context = mock(PreCreateTableEvent.class);
    when(context.getEventType()).thenReturn(PreEventType.CREATE_TABLE);

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(dbName);
    when(context.getTable()).thenReturn(table);

    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName);
    UserGroupInformation.setLoginUser(ugi);

    rangerAuth.onEvent(context);
  }
}
