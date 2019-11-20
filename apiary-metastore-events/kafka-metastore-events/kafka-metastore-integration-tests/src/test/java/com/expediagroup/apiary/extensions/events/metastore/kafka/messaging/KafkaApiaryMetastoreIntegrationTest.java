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
package com.expediagroup.apiary.extensions.events.metastore.kafka.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.GROUP_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaIntegrationTestUtils.buildPartition;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaIntegrationTestUtils.buildQualifiedTableName;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaIntegrationTestUtils.buildTable;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaIntegrationTestUtils.buildTableParameters;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAddPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAlterPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableAlterTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableDropPartitionEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableDropTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableInsertEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.jackson.JsonMetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener;

@RunWith(MockitoJUnitRunner.class)
public class KafkaApiaryMetastoreIntegrationTest {

  @ClassRule
  public static final SharedKafkaTestResource KAFKA = new SharedKafkaTestResource();

  private static final String TOPIC_NAME = "topic";
  private static Configuration CONF = new Configuration();

  private static KafkaMetaStoreEventListener kafkaMetaStoreEventListener;
  private static KafkaMessageReader kafkaMessageReader;
  
  private @Mock HMSHandler hmsHandler;

  @BeforeClass
  public static void init() {
    CONF.set(BOOTSTRAP_SERVERS.key(), KAFKA.getKafkaConnectString());
    CONF.set(GROUP_ID.key(), "1");
    CONF.set(TOPIC.key(), TOPIC_NAME);
    KAFKA.getKafkaTestUtils().createTopic(TOPIC_NAME, 1, (short) 1);

    kafkaMetaStoreEventListener = new KafkaMetaStoreEventListener(CONF);

    Properties receiverProperties = KafkaMessageReader.kafkaProperties(CONF);
    receiverProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaMessageReader = new KafkaMessageReader(CONF, new JsonMetaStoreEventSerDe(),
      new KafkaConsumer<>(receiverProperties));
  }

  @Test
  public void createTableEvent() {
    CreateTableEvent createTableEvent = new CreateTableEvent(buildTable(), true, hmsHandler);
    kafkaMetaStoreEventListener.onCreateTable(createTableEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableCreateTableEvent.class);
    SerializableCreateTableEvent event = (SerializableCreateTableEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void dropTableEvent() {
    DropTableEvent dropTableEvent = new DropTableEvent(buildTable(), true, false, hmsHandler);
    kafkaMetaStoreEventListener.onDropTable(dropTableEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableDropTableEvent.class);
    SerializableDropTableEvent event = (SerializableDropTableEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void alterTableEvent() {
    AlterTableEvent alterTableEvent = new AlterTableEvent(buildTable("old_table"), buildTable("new_table"), true,
      hmsHandler);
    kafkaMetaStoreEventListener.onAlterTable(alterTableEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableAlterTableEvent.class);
    SerializableAlterTableEvent event = (SerializableAlterTableEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo("database.new_table");
    assertThat(event.getOldTable().getTableName()).isEqualTo("old_table");
    assertThat(event.getNewTable().getTableName()).isEqualTo("new_table");
    assertThat(event.getOldTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getNewTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void addPartitionEvent() {
    AddPartitionEvent addPartitionEvent = new AddPartitionEvent(buildTable(), buildPartition(), true, hmsHandler);
    kafkaMetaStoreEventListener.onAddPartition(addPartitionEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableAddPartitionEvent.class);
    SerializableAddPartitionEvent event = (SerializableAddPartitionEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getPartitions()).isEqualTo(Lists.newArrayList(buildPartition()));
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void dropPartitionEvent() {
    DropPartitionEvent dropPartitionEvent = new DropPartitionEvent(buildTable(), buildPartition(), true, false, hmsHandler);
    kafkaMetaStoreEventListener.onDropPartition(dropPartitionEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableDropPartitionEvent.class);
    SerializableDropPartitionEvent event = (SerializableDropPartitionEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getPartitions()).isEqualTo(Lists.newArrayList(buildPartition()));
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void alterPartitionEvent() {
    Partition oldPartition = buildPartition("old_partition");
    Partition newPartition = buildPartition("new_partition");
    AlterPartitionEvent alterPartitionEvent = new AlterPartitionEvent(oldPartition, newPartition, buildTable(), true,
      hmsHandler);
    kafkaMetaStoreEventListener.onAlterPartition(alterPartitionEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableAlterPartitionEvent.class);
    SerializableAlterPartitionEvent event = (SerializableAlterPartitionEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getTable().getParameters()).isEqualTo(buildTableParameters());
    assertThat(event.getOldPartition()).isEqualTo(oldPartition);
    assertThat(event.getNewPartition()).isEqualTo(newPartition);
    assertThat(event.getStatus()).isEqualTo(true);
  }

  @Test
  public void insertPartitionEvent() throws MetaException, NoSuchObjectException {
    when(hmsHandler.get_table_req(any())).thenReturn(new GetTableResult(buildTable()));
    ArrayList<String> partitionValues = Lists.newArrayList("value1", "value2");
    InsertEvent insertEvent = new InsertEvent(
      "database",
      "table",
      partitionValues,
      new InsertEventRequestData(Lists.newArrayList("file1", "file2")),
      true,
      hmsHandler
    );
    kafkaMetaStoreEventListener.onInsert(insertEvent);
    SerializableListenerEvent result = kafkaMessageReader.next();
    assertThat(result).isInstanceOf(SerializableInsertEvent.class);
    SerializableInsertEvent event = (SerializableInsertEvent) result;
    assertThat(event.getQualifiedTableName()).isEqualTo(buildQualifiedTableName());
    assertThat(event.getStatus()).isEqualTo(true);
  }

}
