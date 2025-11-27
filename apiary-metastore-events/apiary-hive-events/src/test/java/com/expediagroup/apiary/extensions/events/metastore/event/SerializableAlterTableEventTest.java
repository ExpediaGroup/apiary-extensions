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
package com.expediagroup.apiary.extensions.events.metastore.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializableAlterTableEventTest {

  private static final String OLD_DATABASE = "old_db";
  private static final String OLD_TABLE = "old_tbl";

  private @Mock AlterTableEvent alterTableEvent;
  private @Mock Table newTable;
  private @Mock Table oldTable;

  private ApiaryAlterTableEvent event;

  @Before
  public void init() {
    when(oldTable.getDbName()).thenReturn(OLD_DATABASE);
    when(oldTable.getTableName()).thenReturn(OLD_TABLE);
    when(alterTableEvent.getNewTable()).thenReturn(newTable);
    when(alterTableEvent.getOldTable()).thenReturn(oldTable);
    event = new ApiaryAlterTableEvent(alterTableEvent);
  }

  @Test
  public void databaseName() {
    assertThat(event.getDatabaseName()).isEqualTo(OLD_DATABASE);
  }

  @Test
  public void tableName() {
    assertThat(event.getTableName()).isEqualTo(OLD_TABLE);
  }

  @Test
  public void eventType() {
    assertThat(event.getEventType()).isSameAs(EventType.ON_ALTER_TABLE);
  }

  @Test
  public void tables() {
    assertThat(event.getNewTable()).isSameAs(newTable);
    assertThat(event.getOldTable()).isSameAs(oldTable);
  }
}
