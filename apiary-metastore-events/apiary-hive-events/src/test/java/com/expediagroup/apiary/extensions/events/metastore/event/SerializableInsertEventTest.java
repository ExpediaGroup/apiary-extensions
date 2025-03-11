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
package com.expediagroup.apiary.extensions.events.metastore.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class SerializableInsertEventTest {

  private static final String DATABASE = "db";
  private static final String TABLE = "tbl";
  private static final String KEY = "key";
  private static final String VALUE = "val";
  private static final String FILE = "file";
  private static final String CHECKSUM = "checksum";

  private @Mock InsertEvent insertEvent;

  private ApiaryInsertEvent event;

  @Before
  public void init() {
    Table table = new Table();
    table.setTableName(TABLE);
    table.setDbName(DATABASE);
    table
        .setPartitionKeys(
            Arrays.asList(new FieldSchema(KEY, "string", "")));
    Partition partition = new Partition();
    partition.setValues(Arrays.asList(VALUE));
    when(insertEvent.getTableObj()).thenReturn(table);
    when(insertEvent.getPartitionObj()).thenReturn(partition);
    when(insertEvent.getFiles()).thenReturn(ImmutableList.of(FILE));
    when(insertEvent.getFileChecksums()).thenReturn(ImmutableList.of(CHECKSUM));
    event = new ApiaryInsertEvent(insertEvent);
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
    assertThat(event.getEventType()).isSameAs(EventType.ON_INSERT);
  }

  @Test
  public void keyValues() {
    assertThat(event.getPartitionKeyValues()).isEqualTo(ImmutableMap.of(KEY, VALUE));
  }

  @Test
  public void files() {
    assertThat(event.getFiles()).isEqualTo(ImmutableList.of(FILE));
  }

  @Test
  public void checksums() {
    assertThat(event.getFileChecksums()).isEqualTo(ImmutableList.of(CHECKSUM));
  }

}
