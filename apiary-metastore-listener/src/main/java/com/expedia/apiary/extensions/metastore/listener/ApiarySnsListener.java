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
package com.expedia.apiary.extensions.metastore.listener;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

/**
 * <p>
 * A simple Hive Metastore Event Listener which spits out Event Information in JSON format to SNS Topic specified
 * through ${SNS_ARN} environment variable.
 * </p>
 */
public class ApiarySnsListener extends MetaStoreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiarySnsListener.class);

  private static final String TOPIC_ARN = System.getenv("SNS_ARN");
  final static String PROTOCOL_VERSION = "1.0";
  private final String protocolVersion = PROTOCOL_VERSION;

  private final AmazonSNS snsClient;

  public ApiarySnsListener(Configuration config) {
    super(config);
    snsClient = AmazonSNSClientBuilder.defaultClient();
    log.debug("ApiarySnsListener created ");
  }

  ApiarySnsListener(Configuration config, AmazonSNS snsClient) {
    super(config);
    this.snsClient = snsClient;
    log.debug("ApiarySnsListener created");
  }

  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishEvent(EventType.CREATE_TABLE, event.getTable(), null, null, null);
  }

  @Override
  public void onDropTable(DropTableEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishEvent(EventType.DROP_TABLE, event.getTable(), null, null, null);
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishEvent(EventType.ALTER_TABLE, event.getNewTable(), event.getOldTable(), null, null);
  }

  @Override
  public void onAddPartition(AddPartitionEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      // TODO: do we want to have a separate event for each partition or one event with all of them?
      publishEvent(EventType.ADD_PARTITION, event.getTable(), null, partitions.next(), null);
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      // TODO: do we want to have a separate event for each partition or one event with all of them?
      publishEvent(EventType.DROP_PARTITION, event.getTable(), null, partitions.next(), null);
    }
  }

  @Override
  public void onInsert(InsertEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishInsertEvent(EventType.INSERT, event.getDb(), event.getTable(), event.getPartitionKeyValues(),
        event.getFiles(), event.getFileChecksums());
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishEvent(EventType.ALTER_PARTITION, event.getTable(), null, event.getNewPartition(), event.getOldPartition());
  }

  private void publishEvent(
      EventType eventType,
      Table table,
      Table oldtable,
      Partition partition,
      Partition oldpartition)
    throws MetaException {
    JSONObject json = createBaseMessage(eventType, table.getDbName(), table.getTableName());

    if (oldtable != null) {
      json.put("oldTableName", oldtable.getTableName());
    }
    if (partition != null) {
      LinkedHashMap<String, String> partitionKeysMap = new LinkedHashMap<>();
      for (FieldSchema fieldSchema : table.getPartitionKeys()) {
        partitionKeysMap.put(fieldSchema.getName(), fieldSchema.getType());
      }

      JSONObject partitionKeys = new JSONObject(partitionKeysMap);
      json.put("partitionKeys", partitionKeys);
      JSONArray partitionValuesArray = new JSONArray(partition.getValues());
      json.put("partitionValues", partitionValuesArray);
    }
    if (oldpartition != null) {
      JSONArray partitionValuesArray = new JSONArray(oldpartition.getValues());
      json.put("oldPartitionValues", partitionValuesArray);
    }

    sendMessage(json);
  }

  private void publishInsertEvent(
      EventType eventType,
      String dbName,
      String tableName,
      Map<String, String> partitionKeyValues,
      List<String> files,
      List<String> fileChecksums) {
    JSONObject json = createBaseMessage(eventType, dbName, tableName);

    JSONArray filesArray = new JSONArray(files);
    json.put("files", filesArray);
    JSONArray fileChecksumsArray = new JSONArray(fileChecksums);
    json.put("fileChecksums", fileChecksumsArray);
    JSONObject partitionKeyValuesObject = new JSONObject(partitionKeyValues);
    json.put("partitionKeyValues", partitionKeyValuesObject);

    sendMessage(json);
  }

  private JSONObject createBaseMessage(EventType eventType, String dbName, String tableName) {
    JSONObject json = new JSONObject();
    json.put("protocolVersion", protocolVersion);
    json.put("eventType", eventType.toString());
    json.put("dbName", dbName);
    json.put("tableName", tableName);
    return json;
  }

  private void sendMessage(JSONObject json) {
    String msg = json.toString();
    PublishRequest publishRequest = new PublishRequest(TOPIC_ARN, msg);
    log.debug(String.format("Sending Message: {} to {}", msg, TOPIC_ARN));
    PublishResult publishResult = snsClient.publish(publishRequest);
    // TODO: check on size of message and truncation etc (this can come later if/when we add more)
    log.debug("Published SNS Message - " + publishResult.getMessageId());
  }
}
