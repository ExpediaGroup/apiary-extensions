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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SerializableAlterPartitionEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";

  private @Mock AlterPartitionEvent alterPartitionEvent;
  private @Mock Table table;
  private @Mock Partition newPartition;
  private @Mock Partition oldPartition;

  private ApiaryAlterPartitionEvent event;

  @Before
  public void init() {
    when(table.getDbName()).thenReturn(DATABASE);
    when(table.getTableName()).thenReturn(TABLE);
    when(alterPartitionEvent.getTable()).thenReturn(table);
    when(alterPartitionEvent.getNewPartition()).thenReturn(newPartition);
    when(alterPartitionEvent.getOldPartition()).thenReturn(oldPartition);
    event = new ApiaryAlterPartitionEvent(alterPartitionEvent);
  }

  @Test
  public void databaseName() {
    assertThat(event.getDatabaseName()).isEqualTo(DATABASE);
  }

  @Test
  public void tableName() {
    assertThat(event.getTableName()).isEqualTo(TABLE);
  }

  @Test
  public void eventType() {
    assertThat(event.getEventType()).isSameAs(EventType.ON_ALTER_PARTITION);
  }

  @Test
  public void table() {
    assertThat(event.getTable()).isSameAs(table);
  }

  @Test
  public void partitions() {
    assertThat(event.getNewPartition()).isSameAs(newPartition);
    assertThat(event.getOldPartition()).isSameAs(oldPartition);
  }

}
