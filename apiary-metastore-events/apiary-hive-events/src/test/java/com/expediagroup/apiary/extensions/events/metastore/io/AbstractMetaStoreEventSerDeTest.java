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
package com.expediagroup.apiary.extensions.events.metastore.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static com.expediagroup.apiary.extensions.events.metastore.io.SerDeTestUtils.DATABASE;
import static com.expediagroup.apiary.extensions.events.metastore.io.SerDeTestUtils.TABLE;
import static com.expediagroup.apiary.extensions.events.metastore.io.SerDeTestUtils.createEnvironmentContext;
import static com.expediagroup.apiary.extensions.events.metastore.io.SerDeTestUtils.createPartition;
import static com.expediagroup.apiary.extensions.events.metastore.io.SerDeTestUtils.createTable;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAddPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryAlterTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryDropTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryInsertEvent;
import com.expediagroup.apiary.extensions.events.metastore.event.ApiaryListenerEvent;

@RunWith(Parameterized.class)
public abstract class AbstractMetaStoreEventSerDeTest {

  private static InsertEventRequestData mockInsertEventRequestData() {
    InsertEventRequestData eventRequestData = mock(InsertEventRequestData.class);
    when(eventRequestData.getFilesAdded()).thenReturn(Arrays.asList("file_0000"));
    return eventRequestData;
  }

  private static HMSHandler mockHandler() throws Exception {
    GetTableResult getTableResult = mock(GetTableResult.class);
    when(getTableResult.getTable()).thenReturn(createTable());
    HMSHandler handler = mock(HMSHandler.class);
    when(handler.get_table_req(any(GetTableRequest.class))).thenReturn(getTableResult);
    return handler;
  }

  private static ApiaryCreateTableEvent serializableCreateTableEvent() throws Exception {
    CreateTableEvent event = new CreateTableEvent(createTable(), true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryCreateTableEvent(event);
  }

  private static ApiaryAlterTableEvent serializableAlterTableEvent() throws Exception {
    AlterTableEvent event = new AlterTableEvent(createTable(), createTable(new FieldSchema("new_col", "string", null)),
        true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryAlterTableEvent(event);
  }

  private static ApiaryDropTableEvent serializableDropTableEvent() throws Exception {
    DropTableEvent event = new DropTableEvent(createTable(), true, false, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryDropTableEvent(event);
  }

  private static ApiaryAddPartitionEvent serializableAddPartitionEvent() throws Exception {
    AddPartitionEvent event = new AddPartitionEvent(createTable(), createPartition("a"), true, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryAddPartitionEvent(event);
  }

  private static ApiaryAlterPartitionEvent serializableAlterPartitionEvent() throws Exception {
    AlterPartitionEvent event = new AlterPartitionEvent(createPartition("a"), createPartition("b"), createTable(), true,
        mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryAlterPartitionEvent(event);
  }

  private static ApiaryDropPartitionEvent serializableDropPartitionEvent() throws Exception {
    DropPartitionEvent event = new DropPartitionEvent(createTable(), createPartition("a"), true, false, mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryDropPartitionEvent(event);
  }

  private static ApiaryInsertEvent serializableInsertEvent() throws Exception {
    InsertEvent event = new InsertEvent(DATABASE, TABLE, Arrays.asList("a"), mockInsertEventRequestData(), true,
        mockHandler());
    event.setEnvironmentContext(createEnvironmentContext());
    return new ApiaryInsertEvent(event);
  }

  @Parameters(name = "{index}: {0}")
  public static ApiaryListenerEvent[] data() throws Exception {
    return new ApiaryListenerEvent[] {
        serializableCreateTableEvent(),
        serializableAlterTableEvent(),
        serializableDropTableEvent(),
        serializableAddPartitionEvent(),
        serializableAlterPartitionEvent(),
        serializableDropPartitionEvent(),
        serializableInsertEvent() };
  }

  public @Parameter ApiaryListenerEvent event;

  protected abstract MetaStoreEventSerDe serDe();

  @Test
  public void typical() throws Exception {
    ApiaryListenerEvent processedEvent = serDe().unmarshal(serDe().marshal(event));
    assertThat(processedEvent).isNotSameAs(event).isEqualTo(event);
  }

}
