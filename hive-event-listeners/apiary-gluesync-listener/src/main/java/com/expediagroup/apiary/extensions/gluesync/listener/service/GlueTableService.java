/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener.service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;

public class GlueTableService {
  private static final Logger log = LoggerFactory.getLogger(GlueTableService.class);
  public static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";
  private static final int DEFAULT_MAX_RESULTS_SIZE = 1000; // Current max supported by Glue
  private static final int DEFAULT_COPY_PARTITION_BATCH_SIZE = 1000;

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;

  public GlueTableService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table) {
    CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));
    glueClient.createTable(createTableRequest);
    log.info(table + " table created in glue catalog");
  }

  public void update(Table table) {
    boolean skipArchive = shouldSkipArchive(table);

    UpdateTableRequest updateTableRequest = new UpdateTableRequest()
        .withSkipArchive(skipArchive)
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));
    glueClient.updateTable(updateTableRequest);
    log.info(table + " table updated in glue catalog");
  }

  public void delete(Table table) {
    DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
        .withName(table.getTableName())
        .withDatabaseName(transformer.glueDbName(table));
    glueClient.deleteTable(deleteTableRequest);
    log.info(table + " table deleted from glue catalog");
  }

  /**
   * Function to copy partitions using BatchCreatePartitionRequest but knowing the partition input list limit is 100,
   * however input parameter can have larger list
    */
  public void copyPartitions(Table table, List<Partition> partitions) {
    List<PartitionInput> partitionInputs = partitions.stream()
        .map(this::convertToPartitionInput)
        .collect(Collectors.toList());
    int partitionCount = partitionInputs.size();
    int batchSize = DEFAULT_COPY_PARTITION_BATCH_SIZE;
    for (int i = 0; i < partitionCount; i += batchSize) {
      int end = Math.min(i + batchSize, partitionCount);
      List<PartitionInput> batch = partitionInputs.subList(i, end);
      BatchCreatePartitionRequest batchCreatePartitionRequest = new BatchCreatePartitionRequest()
          .withDatabaseName(transformer.glueDbName(table))
          .withTableName(table.getTableName())
          .withPartitionInputList(batch);
      glueClient.batchCreatePartition(batchCreatePartitionRequest);
    }
  }

  /**
   * Function to return all partitions from a table, considering it could reach max results from request and
   * then convert that into PartitionInput
   */
  public List<com.amazonaws.services.glue.model.Partition> getPartitions(Table table) {
    List<com.amazonaws.services.glue.model.Partition> partitions = new ArrayList<>();
    String nextToken = null;
    do {
      GetPartitionsRequest getPartitionsRequest = new GetPartitionsRequest()
          .withDatabaseName(transformer.glueDbName(table))
          .withTableName(table.getTableName())
          .withMaxResults(DEFAULT_MAX_RESULTS_SIZE)
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

  private PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition partition) {
    return new PartitionInput()
        .withValues(partition.getValues())
        .withStorageDescriptor(partition.getStorageDescriptor())
        .withParameters(partition.getParameters());
  }
}
