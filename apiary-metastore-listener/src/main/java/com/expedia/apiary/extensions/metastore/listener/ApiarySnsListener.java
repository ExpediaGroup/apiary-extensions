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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
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
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

/**
 * TODO: add some high level javadoc
 */
public class ApiarySnsListener extends MetaStoreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiarySnsListener.class);

  private static final String TOPIC_ARN = System.getenv("SNS_ARN");
  final static String PROTOCOL_VERSION = "1.0";
  private final String protocolVersion = PROTOCOL_VERSION;

  private AmazonSNS snsClient;

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
      publishEvent(EventType.DROP_PARTITION, event.getTable(), null, partitions.next(), null);
    }
  }

  @Override
  public void onInsert(InsertEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    // TODO: should the event type we send through for this be ALTER_PARTITION? or do we need a new
    // INSERT event type?
    // TODO: we don't have a partition available in the InsertEvent here, but we do have partition
    // key values, do we want these? (if so will need another method below)
    // publishEvent(EventType.???, event.getTable(), null, event.getPartitionKeyValues());
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    if (event.getStatus() == false) {
      return;
    }
    publishEvent(EventType.ALTER_PARTITION, event.getTable(), null, event.getNewPartition(), event.getOldPartition());
  }

  private void publishEvent(EventType eventType, Table table, Table oldtable, Partition partition, Partition oldpartition)
      throws MetaException {
    JSONObject json = new JSONObject();
    json.put("protocolVersion", protocolVersion);
    json.put("eventType", eventType.toString());
    json.put("dbName", table.getDbName());
    json.put("tableName", table.getTableName());
    if (oldtable != null) {
      json.put("oldTableName", oldtable.getTableName());
    }
    if (partition != null) {
      json.put("partition", partition.getValues());
    }
    if (oldpartition != null) {
      json.put("oldPartition", oldpartition.getValues());
    }
    String msg = json.toString();

    PublishRequest publishRequest = new PublishRequest(TOPIC_ARN, msg);
    log.error(publishRequest.getTopicArn());
    PublishResult publishResult = snsClient.publish(publishRequest);
    // TODO: check on size of message and truncation etc (this can come later if/when we add more)
    log.debug("Published SNS Message - " + publishResult.getMessageId());
  }
}
