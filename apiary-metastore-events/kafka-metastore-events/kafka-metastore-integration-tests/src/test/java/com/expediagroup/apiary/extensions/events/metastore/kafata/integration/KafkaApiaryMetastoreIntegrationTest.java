package com.expediagroup.apiary.extensions.events.metastore.kafata.integration;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaConsumerProperty.GROUP_ID;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.BOOTSTRAP_SERVERS;
import static com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaProducerProperty.TOPIC;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableCreateTableEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.event.SerializableListenerEvent;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.jackson.JsonMetaStoreEventSerDe;
import com.expediagroup.apiary.extensions.events.metastore.kafka.listener.KafkaMetaStoreEventListener;
import com.expediagroup.apiary.extensions.events.metastore.kafka.messaging.KafkaMessageReader;

public class KafkaApiaryMetastoreIntegrationTest {

  private static Configuration CONF = new Configuration();

  @ClassRule
  public static final SharedKafkaTestResource KAFKA = new SharedKafkaTestResource();

  @BeforeClass
  public static void init() {
    CONF.set(BOOTSTRAP_SERVERS.key(), KAFKA.getKafkaConnectString());
    CONF.set(GROUP_ID.key(), "1");
    CONF.set(TOPIC.key(), "topic");
  }

  @Test
  public void typical() {
    KafkaMetaStoreEventListener metastoreListener = new KafkaMetaStoreEventListener(CONF);
    KafkaMessageReader messageReader = new KafkaMessageReader(CONF, new JsonMetaStoreEventSerDe());

    CreateTableEvent createTableEvent = new CreateTableEvent(buildTable(), true, null);
    metastoreListener.onCreateTable(createTableEvent);

    SerializableListenerEvent message = messageReader.next();
    assertThat(message.getEventType()).isEqualTo(SerializableCreateTableEvent.class);
  }

  private Table buildTable() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put("key1", "value1");
    parameters.put("key2", "value2");
    List<FieldSchema> partitions = Lists.newArrayList();
    partitions.add(new FieldSchema("a", "string", "comment"));
    partitions.add(new FieldSchema("b", "string", "comment"));
    partitions.add(new FieldSchema("c", "string", "comment"));
    return new Table(
      "table",
      "database",
      "me",
      1,
      1,
      1,
      new StorageDescriptor(),
      partitions,
      parameters,
      "originalText",
      "expandedTtext",
      "tableType"
      );
  }
}
