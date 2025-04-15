/**
 * Copyright (C) 2018-2024 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueDatabaseService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueTableService;

public class ApiaryGlueSync extends MetaStoreEventListener {

  static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";

  private static final Logger log = LoggerFactory.getLogger(ApiaryGlueSync.class);


  private final AWSGlue glueClient;
  private final String gluePrefix;
  private final GlueDatabaseService glueDatabaseService;
  private final GlueTableService glueTableService;

  public ApiaryGlueSync(Configuration config) {
    super(config);
    glueClient = AWSGlueClientBuilder.standard().withRegion(System.getenv("AWS_REGION")).build();
    gluePrefix = System.getenv("GLUE_PREFIX");
    glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    glueTableService = new GlueTableService(glueClient, gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public ApiaryGlueSync(Configuration config, AWSGlue glueClient, String gluePrefix) {
    super(config);
    this.glueClient = glueClient;
    this.gluePrefix = gluePrefix;
    glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Database database = event.getDatabase();
    try {
      glueDatabaseService.create(database);
    } catch (AlreadyExistsException e) {
      log.info(database + " database already exists in glue, updating....");
      glueDatabaseService.update(database);
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Database database = event.getDatabase();
    glueDatabaseService.delete(database);
  }

  @Override
  public void onCreateTable(CreateTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    try {
      CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.createTable(createTableRequest);
      log.info(table + " table created in glue catalog");
    } catch (AlreadyExistsException e) {
      log.info(table + " table already exists in glue, updating....");
      UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.updateTable(updateTableRequest);
      log.info(table + " table updated in glue catalog");
    }
  }

  @Override
  public void onDropTable(DropTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    try {
      DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
          .withName(table.getTableName())
          .withDatabaseName(glueDbName(table));
      glueClient.deleteTable(deleteTableRequest);
      log.info(table + " table deleted from glue catalog");
    } catch (EntityNotFoundException e) {
      log.info(table + " table doesn't exist in glue catalog");
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table oldTable = event.getOldTable();
    Table newTable = event.getNewTable();
    try {
      // Table rename are not supported by Glue, so we need to delete table and create again
      if (isTableRename(oldTable, newTable)) {
        log.info("{} glue table rename detected to {}", oldTable.getTableName(), newTable.getTableName());
        long startTime = System.currentTimeMillis();
        createTable(newTable);
        copyPartitions(newTable, getPartitions(oldTable));
        deleteTable(oldTable);
        long duration = System.currentTimeMillis() - startTime;
        log.info("{} glue table rename to {} finised in {}ms", oldTable.getTableName(), newTable.getTableName(), duration);
        return;
      }
      boolean skipArchive = shouldSkipArchive(newTable);
      UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withSkipArchive(skipArchive)
          .withTableInput(transformTable(newTable))
          .withDatabaseName(glueDbName(newTable));
      glueClient.updateTable(updateTableRequest);
      log.info(newTable + " table updated in glue catalog");
    } catch (EntityNotFoundException e) {
      log.info(newTable + " table doesn't exist in glue, creating....");
      createTable(newTable);
    }
  }

  private boolean isTableRename(Table oldTable, Table newTable) {
    return !oldTable.getTableName().equals(newTable.getTableName());
  }

  private void createTable(Table table) {
    CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableInput(transformTable(table))
        .withDatabaseName(glueDbName(table));
    glueClient.createTable(createTableRequest);
    log.info(table + " table created in glue catalog");
  }

  private void deleteTable(Table table) {
    DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
        .withName(table.getTableName())
        .withDatabaseName(glueDbName(table));
    glueClient.deleteTable(deleteTableRequest);
    log.info(table + " table deleted from glue catalog");
  }

  // Function to copy partitions using BatchCreatePartitionRequest but knowing the partition input list limit is 100, however input parameter can have larger list
  private void copyPartitions(Table table, List<com.amazonaws.services.glue.model.Partition> partitions) {
    List<PartitionInput> partitionInputs = partitions.stream()
        .map(this::convertToPartitionInput)
        .collect(Collectors.toList());
    int partitionCount = partitionInputs.size();
    int batchSize = 100;
    for (int i = 0; i < partitionCount; i += batchSize) {
      int end = Math.min(i + batchSize, partitionCount);
      List<PartitionInput> batch = partitionInputs.subList(i, end);
      BatchCreatePartitionRequest batchCreatePartitionRequest = new BatchCreatePartitionRequest()
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName())
          .withPartitionInputList(batch);
      glueClient.batchCreatePartition(batchCreatePartitionRequest);
    }
  }

  private PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition partition) {
    return new PartitionInput()
        .withValues(partition.getValues())
        .withStorageDescriptor(partition.getStorageDescriptor())
        .withParameters(partition.getParameters());
  }

  // Function to return all partitions from a table, considering it could reach max results from request and then convert that into PartitionInput
  private List<com.amazonaws.services.glue.model.Partition> getPartitions(Table table) {
    List<com.amazonaws.services.glue.model.Partition> partitions = new ArrayList<>();
    String nextToken = null;
    do {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest()
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName())
          .withMaxResults(1000)
          .withNextToken(nextToken);
      com.amazonaws.services.glue.model.GetPartitionsResult result = glueClient.getPartitions(getPartitionsRequest);
      if (result != null) {
        partitions.addAll(result.getPartitions());
        nextToken = result.getNextToken();
      }
    } while (nextToken != null);
    return partitions;
  }

  private boolean shouldSkipArchive(Table table) {
    boolean skipArchive = true;
    if (table.getParameters() != null) {
      //Only if explicitly overridden to false do enable table archive. Normally we want to skip archiving. 
      String skipArchiveParam = table.getParameters().get(APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM);
      if ("false".equals(skipArchiveParam)) {
        skipArchive = false;
      }
    }
    return skipArchive;
  }

  @Override
  public void onAddPartition(AddPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
            .withPartitionInput(transformPartition(partition))
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.createPartition(createPartitionRequest);
      } catch (AlreadyExistsException e) {
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
            .withPartitionValueList(transformPartition(partition).getValues())
            .withPartitionInput(transformPartition(partition))
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.updatePartition(updatePartitionRequest);
      }
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest()
            .withPartitionValues(transformPartition(partition).getValues())
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.deletePartition(deletePartitionRequest);
      } catch (EntityNotFoundException ignored) {}
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Partition partition = event.getNewPartition();
    try {
      UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
          .withPartitionValueList(transformPartition(partition).getValues())
          .withPartitionInput(transformPartition(partition))
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName());
      glueClient.updatePartition(updatePartitionRequest);
    } catch (EntityNotFoundException e) {
      CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
          .withPartitionInput(transformPartition(partition))
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName());
      glueClient.createPartition(createPartitionRequest);
    }
  }
}
