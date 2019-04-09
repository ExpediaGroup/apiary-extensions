/**
 * Copyright (C) 2018-2019 Expedia Inc.
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
package com.expediagroup.apiary.extensions.receiver.common.messaging;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.assertj.core.api.Assertions.assertThat;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.expediagroup.apiary.extensions.receiver.common.event.AddPartitionEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.AlterPartitionEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.AlterTableEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.CreateTableEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.DropPartitionEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.DropTableEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.EventType;
import com.expediagroup.apiary.extensions.receiver.common.event.InsertTableEvent;
import com.expediagroup.apiary.extensions.receiver.common.event.ListenerEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@RunWith(MockitoJUnitRunner.class)
public class JsonMetaStoreEventDeserializerTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final Map<String, String> PARTITION_KEYS_MAP = ImmutableMap
      .of("col_1", "string", "col_2", "integer", "col_3", "string");
  private static final List<String> PARTITION_VALUES = ImmutableList.of("val_1", "val_2", "val_3");
  private static final List<String> OLD_PARTITION_VALUES = ImmutableList.of("val_4", "val_5", "val_6");

  private static final Map<String, String> TABLE_PARAM_MAP = ImmutableMap.of("param_1", "val_1", "param_2", "val_2");

  private static final String TEST_DB = "some_db";
  private static final String TEST_TABLE = "some_table";
  private static final String TEST_TABLE_LOCATION = "s3://table_location";
  private static final String OLD_TEST_TABLE_LOCATION = "s3://old_table_location";
  private static final String PARTITION_LOCATION = "s3://table_location/partition_location";
  private static final String OLD_PARTITION_LOCATION = "s3://table_location/old_partition_location";

  @Rule
  public DataFolder dataFolder = new ClassDataFolder();

  private final JsonMetaStoreEventDeserializer metaStoreEventDeserializer = new JsonMetaStoreEventDeserializer(OBJECT_MAPPER);

  private String addPartitionEvent;
  private String alterPartitionEvent;
  private String createTableEvent;
  private String dropPartitionEvent;
  private String insertEvent;
  private String alterTableEvent;
  private String dropTableEvent;

  @Before
  public void init() throws IOException {
    addPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("add_partition.json").toPath()), UTF_8);
    alterPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("alter_partition.json").toPath()), UTF_8);
    dropPartitionEvent = new String(Files.readAllBytes(dataFolder.getFile("drop_partition.json").toPath()), UTF_8);
    createTableEvent = new String(Files.readAllBytes(dataFolder.getFile("create_table.json").toPath()), UTF_8);
    insertEvent = new String(Files.readAllBytes(dataFolder.getFile("insert_table.json").toPath()), UTF_8);
    alterTableEvent = new String(Files.readAllBytes(dataFolder.getFile("alter_table.json").toPath()), UTF_8);
    dropTableEvent = new String(Files.readAllBytes(dataFolder.getFile("drop_table.json").toPath()), UTF_8);
  }

  @Test
  public void addPartitionEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(addPartitionEvent);
    AddPartitionEvent addPartitionEvent = (AddPartitionEvent) processedEvent;

    assertThat(addPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(addPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(addPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(addPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(addPartitionEvent.getEventType()).isEqualTo(EventType.ADD_PARTITION);
    assertThat(addPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(addPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(addPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(addPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void alterPartitionEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(alterPartitionEvent);
    AlterPartitionEvent alterPartitionEvent = (AlterPartitionEvent) processedEvent;

    assertThat(alterPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(alterPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(alterPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterPartitionEvent.getEventType()).isEqualTo(EventType.ALTER_PARTITION);
    assertThat(alterPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(alterPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(alterPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(alterPartitionEvent.getOldPartitionValues()).isEqualTo(OLD_PARTITION_VALUES);
    assertThat(alterPartitionEvent.getOldPartitionLocation()).isEqualTo(OLD_PARTITION_LOCATION);
    assertThat(alterPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void dropPartitionEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(dropPartitionEvent);
    DropPartitionEvent dropPartitionEvent = (DropPartitionEvent) processedEvent;

    assertThat(dropPartitionEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropPartitionEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropPartitionEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(dropPartitionEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropPartitionEvent.getEventType()).isEqualTo(EventType.DROP_PARTITION);
    assertThat(dropPartitionEvent.getPartitionKeys()).isEqualTo(PARTITION_KEYS_MAP);
    assertThat(dropPartitionEvent.getPartitionValues()).isEqualTo(PARTITION_VALUES);
    assertThat(dropPartitionEvent.getPartitionLocation()).isEqualTo(PARTITION_LOCATION);
    assertThat(dropPartitionEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void createTableEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(createTableEvent);
    CreateTableEvent createTableEvent = (CreateTableEvent) processedEvent;

    assertThat(createTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(createTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(createTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(createTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(createTableEvent.getEventType()).isEqualTo(EventType.CREATE_TABLE);
    assertThat(createTableEvent.getTableParameters()).isEqualTo(Maps.newHashMap());
  }

  @Test
  public void insertTableEvent() throws Exception {
    List<String> expectedFiles = ImmutableList.of("file:/a/b.txt", "file:/a/c.txt");
    List<String> expectedFileChecksums = ImmutableList.of("123", "456");
    Map<String, String> PARTITION_KEY_VALUE_MAP = ImmutableMap.of("col_1", "val_1", "col_2", "val_2", "col_3", "val_3");

    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(insertEvent);
    InsertTableEvent insertTableEvent = (InsertTableEvent) processedEvent;

    assertThat(insertTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(insertTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(insertTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(insertTableEvent.getEventType()).isEqualTo(EventType.INSERT);
    assertThat(insertTableEvent.getPartitionKeyValues()).isEqualTo(PARTITION_KEY_VALUE_MAP);

    assertThat(insertTableEvent.getFiles()).isEqualTo(expectedFiles);
    assertThat(insertTableEvent.getFileChecksums()).isEqualTo(expectedFileChecksums);
  }

  @Test
  public void alterTableEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(alterTableEvent);
    AlterTableEvent alterTableEvent = (AlterTableEvent) processedEvent;

    assertThat(alterTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(alterTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(alterTableEvent.getOldTableLocation()).isEqualTo(OLD_TEST_TABLE_LOCATION);
    assertThat(alterTableEvent.getOldTableName()).isEqualTo(TEST_TABLE);
    assertThat(alterTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(alterTableEvent.getEventType()).isEqualTo(EventType.ALTER_TABLE);
    assertThat(alterTableEvent.getTableParameters()).isEqualTo(TABLE_PARAM_MAP);
  }

  @Test
  public void dropTableEvent() throws Exception {
    ListenerEvent processedEvent = metaStoreEventDeserializer.unmarshal(dropTableEvent);
    DropTableEvent dropTableEvent = (DropTableEvent) processedEvent;

    assertThat(dropTableEvent.getDbName()).isEqualTo(TEST_DB);
    assertThat(dropTableEvent.getTableName()).isEqualTo(TEST_TABLE);
    assertThat(dropTableEvent.getTableLocation()).isEqualTo(TEST_TABLE_LOCATION);
    assertThat(dropTableEvent.getProtocolVersion()).isEqualTo("1.0");
    assertThat(dropTableEvent.getEventType()).isEqualTo(EventType.DROP_TABLE);
    assertThat(dropTableEvent.getTableParameters()).isEqualTo(Maps.newHashMap());
  }
}
