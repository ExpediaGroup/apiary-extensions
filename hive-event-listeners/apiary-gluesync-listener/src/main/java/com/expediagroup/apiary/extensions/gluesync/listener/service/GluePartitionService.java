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
package com.expediagroup.apiary.extensions.gluesync.listener.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequestEntry;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.ValidationException;

public class GluePartitionService {
  private static final Logger log = LoggerFactory.getLogger(GluePartitionService.class);

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();
  private final HiveToGluePartitionComparator partitionComparator = new HiveToGluePartitionComparator();
  public static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";
  private static final int DEFAULT_MAX_RESULTS_SIZE = 1000; // Current max supported by Glue
  private static final int MAX_PARTITION_CREATE_BATCH_SIZE = 100;
  private static final int MAX_PARTITION_UPDATE_BATCH_SIZE = 100;
  private static final int MAX_PARTITION_DELETE_BATCH_SIZE = 25;

  public GluePartitionService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table, org.apache.hadoop.hive.metastore.api.Partition partition) {
    CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    try {
      glueClient.createPartition(createPartitionRequest);
      log.debug("{} partition created in glue catalog", partition);

    } catch (ValidationException | InvalidInputException e) {
      PartitionInput partitionInput = createPartitionRequest.getPartitionInput();
      createPartitionRequest.setPartitionInput(cleanUpPartition(partitionInput));
      glueClient.createPartition(createPartitionRequest);
      log.debug("{} partition created in glue catalog", partition);
    }
  }

  public void update(Table table, org.apache.hadoop.hive.metastore.api.Partition partition) {
    UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
        .withPartitionValueList(transformer.transformPartition(partition).getValues())
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    try {
      glueClient.updatePartition(updatePartitionRequest);
      log.debug("{} partition updated in glue catalog", partition);
    } catch (ValidationException | InvalidInputException e) {
      PartitionInput partitionInput = updatePartitionRequest.getPartitionInput();
      updatePartitionRequest.setPartitionInput(cleanUpPartition(partitionInput));
      glueClient.updatePartition(updatePartitionRequest);
      log.debug("{} partition updated in glue catalog", partition);
    }
  }

  public void delete(Table table, org.apache.hadoop.hive.metastore.api.Partition partition) {
    DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest()
        .withPartitionValues(transformer.transformPartition(partition).getValues())
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    glueClient.deletePartition(deletePartitionRequest);
    log.debug("{} partition deleted from glue catalog", partition);
  }

  private PartitionInput cleanUpPartition(PartitionInput partition) {
    log.debug("Cleaning up partition comments manually on {} to resolve validation", partition);
    long startTime = System.currentTimeMillis();
    PartitionInput result = cleaner.cleanPartition(partition);
    long duration = System.currentTimeMillis() - startTime;
    log.debug("Clean up partition comments operation on {} finished in {}ms", partition, duration);
    return result;
  }

  /**
   * Function to copy partitions using BatchCreatePartitionRequest.
   */
  public void copyPartitions(Table table, List<Partition> partitions) {
    List<PartitionInput> partitionInputs = partitions.stream()
        .map(this::convertToPartitionInput)
        .collect(Collectors.toList());

    executeBatched(partitionInputs, MAX_PARTITION_CREATE_BATCH_SIZE,
        batch -> new BatchCreatePartitionRequest()
            .withDatabaseName(transformer.glueDbName(table))
            .withTableName(table.getTableName())
            .withPartitionInputList(batch),
        req -> glueClient.batchCreatePartition((BatchCreatePartitionRequest) req),
        "Copying");
  }

  /**
   * Function to return all partitions from a table, considering it could reach
   * max results from request and
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

  public boolean shouldSkipArchive(Table table) {
    boolean skipArchive = true;
    if (table.getParameters() != null) {
      // Only if explicitly overridden to false do enable table archive. Normally we
      // want to skip archiving.
      String skipArchiveParam = table.getParameters().get(APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM);
      if ("false".equals(skipArchiveParam)) {
        skipArchive = false;
      }
    }
    return skipArchive;
  }

  public PartitionInput convertToPartitionInput(com.amazonaws.services.glue.model.Partition partition) {
    return new PartitionInput()
        .withValues(partition.getValues())
        .withStorageDescriptor(partition.getStorageDescriptor())
        .withParameters(partition.getParameters());
  }

  /**
   * Generic helper to execute AWS Glue batch operations with dynamic batch size
   * reduction on 413 (Payload Too Large) errors.
   *
   * @param items            the list of items to process in batches
   * @param initialBatchSize starting batch size
   * @param requestBuilder   builds the request object from a batch of items
   * @param requestExecutor  executes the request against AWS Glue
   * @param actionVerb       used for logging (e.g. "Creating", "Updating",
   *                         "Deleting")
   */
  private <T, R> void executeBatched(
      List<T> items,
      int initialBatchSize,
      Function<List<T>, R> requestBuilder,
      Consumer<R> requestExecutor,
      String actionVerb) {
    if (items == null || items.isEmpty()) {
      return;
    }

    int batchSize = initialBatchSize;
    int size = items.size();
    for (int i = 0; i < size;) {
      int end = Math.min(i + batchSize, size);
      List<T> batch = items.subList(i, end);
      R request = requestBuilder.apply(batch);
      try {
        log.debug("{} {} partitions with batch size {}", actionVerb, batch.size(), batchSize);
        requestExecutor.accept(request);
        i += batchSize;
      } catch (AmazonServiceException e) {
        if (e.getStatusCode() == 413 && batchSize > 1) {
          batchSize = Math.max(1, batchSize / 2);
          log.warn("Payload too large (413) when {} {} partitions. Reducing batch size to {} and retrying",
              actionVerb.toLowerCase(), batch.size(), batchSize);
        } else {
          throw e;
        }
      }
    }
  }

  /*
   * This method takes a dbName, a tableName and a list of hive partition objects,
   * as well as a flag to
   * indicate whether to do deletions.
   * It fetches and maps the glue partitions to the hive partitions creating
   * the following groups:
   * - partitions that exist in both glue and hive (and test as unequal) - for
   * update
   * - partitions that exist only in glue - for delete
   * - partitions that exist only in hive - for create
   * Then it uses batch update functions on the glue api to create, update, or
   * delete glue partitions in order to match the hive partitions.
   * delete should only be performed if the flag deletePartitions is set to true
   * cli.
   */
  public void synchronizePartitions(
      Table table,
      List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions,
      boolean deletePartitions,
      boolean verbose) {
    List<Partition> gluePartitions = getPartitions(table);

    // Map Glue partition values to Glue Partition object
    HashMap<List<String>, Partition> gluePartitionMap = new HashMap<>();
    for (Partition gluePartition : gluePartitions) {
      gluePartitionMap.put(gluePartition.getValues(), gluePartition);
    }

    // Map Hive partition values to Hive Partition object
    HashMap<List<String>, org.apache.hadoop.hive.metastore.api.Partition> hivePartitionMap = new HashMap<>();
    for (org.apache.hadoop.hive.metastore.api.Partition hivePartition : hivePartitions) {
      hivePartitionMap.put(hivePartition.getValues(), hivePartition);
    }

    // Partitions to create, update, delete
    List<org.apache.hadoop.hive.metastore.api.Partition> partitionsToCreate = new ArrayList<>();
    HashMap<org.apache.hadoop.hive.metastore.api.Partition, Partition> partitionsToUpdate = new HashMap<>();
    List<Partition> partitionsToDelete = new ArrayList<>();

    int skippedCount = 0;
    // Find partitions to create or update
    for (org.apache.hadoop.hive.metastore.api.Partition hivePartition : hivePartitions) {
      List<String> values = hivePartition.getValues();
      Partition gluePartition = gluePartitionMap.get(values);
      if (gluePartition != null) {
        // Exists in both: check if they are equal
        if (partitionComparator.equals(hivePartition, gluePartition)) {
          skippedCount++;
        } else {
          // Not equal: update
          partitionsToUpdate.put(hivePartition, gluePartition);
        }
      } else {
        // Exists only in Hive: create
        partitionsToCreate.add(hivePartition);
      }
    }

    log.debug("Partitions {} partitions to create, {} partitions to update, {} partitions unchanged",
        partitionsToCreate.size(),
        partitionsToUpdate.size(),
        skippedCount);
    batchCreatePartitions(table, partitionsToCreate);
    batchUpdatePartitions(table, partitionsToUpdate);

    log.debug("Pre delete: hive part count {} glue part count {}", hivePartitions.size(),
        gluePartitions.size());

    // Find partitions to delete (only if flag is set)
    if (deletePartitions) {
      for (Partition gluePartition : gluePartitions) {
        List<String> values = gluePartition.getValues();
        if (!hivePartitionMap.containsKey(values)) {
          // Exists only in Glue: delete
          partitionsToDelete.add(gluePartition);
        }
      }
      log.debug("Found {} partitions to delete", partitionsToDelete.size());
      batchDeletePartitions(table, partitionsToDelete);
    }
  }

  /**
   * Creates partitions in AWS Glue in batches for the specified Hive table.
   * <p>
   * This method transforms and cleans the provided Hive partitions, then sends
   * them to Glue in batches to avoid exceeding the maximum allowed batch size.
   * If the input list is null or empty, the method returns immediately. If a 413
   * (Payload Too Large) error occurs, the batch size is dynamically reduced and
   * the operation is retried.
   */
  protected void batchCreatePartitions(Table table,
      List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions) {
    if (hivePartitions == null || hivePartitions.isEmpty()) {
      return;
    }
    List<PartitionInput> partitionInputs = hivePartitions.stream()
        .map(transformer::transformPartition)
        .map(cleaner::cleanPartition)
        .collect(Collectors.toList());

    executeBatched(partitionInputs, MAX_PARTITION_CREATE_BATCH_SIZE,
        batch -> new BatchCreatePartitionRequest()
            .withDatabaseName(transformer.glueDbName(table))
            .withTableName(table.getTableName())
            .withPartitionInputList(batch),
        req -> glueClient.batchCreatePartition((BatchCreatePartitionRequest) req),
        "Creating");
  }

  /**
   * Updates multiple partitions in AWS Glue in batches.
   * <p>
   * This method transforms and cleans each Hive partition, then creates a batch
   * update request for AWS Glue. The updates are performed in batches to avoid
   * exceeding the maximum allowed batch size. If a 413 (Payload Too Large) error
   * occurs, the batch size is dynamically reduced and the operation is retried.
   */
  protected void batchUpdatePartitions(
      Table table,
      HashMap<org.apache.hadoop.hive.metastore.api.Partition, Partition> partitionsToUpdate) {
    if (partitionsToUpdate == null || partitionsToUpdate.isEmpty()) {
      return;
    }

    List<BatchUpdatePartitionRequestEntry> entries = new ArrayList<>();
    for (java.util.Map.Entry<org.apache.hadoop.hive.metastore.api.Partition, Partition> entry : partitionsToUpdate
        .entrySet()) {
      org.apache.hadoop.hive.metastore.api.Partition hivePartition = entry.getKey();
      Partition gluePartition = entry.getValue();
      PartitionInput partitionInput = transformer.transformPartition(hivePartition);
      PartitionInput partitionInputCleaned = cleaner.cleanPartition(partitionInput);

      BatchUpdatePartitionRequestEntry updateEntry = new BatchUpdatePartitionRequestEntry()
          .withPartitionValueList(gluePartition.getValues())
          .withPartitionInput(partitionInputCleaned);

      entries.add(updateEntry);
    }

    executeBatched(entries, MAX_PARTITION_UPDATE_BATCH_SIZE,
        batch -> new BatchUpdatePartitionRequest()
            .withDatabaseName(transformer.glueDbName(table))
            .withTableName(table.getTableName())
            .withEntries(batch),
        req -> glueClient.batchUpdatePartition((BatchUpdatePartitionRequest) req),
        "Updating");
  }

  /**
   * Deletes multiple partitions from AWS Glue in batches.
   * <p>
   * This method deletes the specified partitions in batches to avoid exceeding
   * the maximum allowed batch size. If a 413 (Payload Too Large) error occurs,
   * the batch size is dynamically reduced and the operation is retried.
   */
  protected void batchDeletePartitions(Table table, List<Partition> partitionsToDelete) {
    if (partitionsToDelete == null || partitionsToDelete.isEmpty()) {
      return;
    }

    List<List<String>> partitionValueLists = partitionsToDelete.stream()
        .map(Partition::getValues)
        .collect(Collectors.toList());

    executeBatched(partitionValueLists, MAX_PARTITION_DELETE_BATCH_SIZE,
        batch -> new BatchDeletePartitionRequest()
            .withDatabaseName(transformer.glueDbName(table))
            .withTableName(table.getTableName())
            .withPartitionsToDelete(batch.stream()
                .map(values -> new com.amazonaws.services.glue.model.PartitionValueList().withValues(values))
                .collect(Collectors.toList())),
        req -> glueClient.batchDeletePartition((BatchDeletePartitionRequest) req),
        "Deleting");
  }
}
