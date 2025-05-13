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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;

public class GluePartitionService {
  private static final Logger log = LoggerFactory.getLogger(GluePartitionService.class);

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();

  public GluePartitionService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table, Partition partition) {
    CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    try {
      glueClient.createPartition(createPartitionRequest);
      log.debug("{} partition created in glue catalog", partition);

    } catch (InvalidInputException e) {
      log.debug("Cleaning up partition manually {} to resolve validation", partition);
      PartitionInput partitionInput = createPartitionRequest.getPartitionInput();
      createPartitionRequest.setPartitionInput(cleaner.cleanPartition(partitionInput));
      glueClient.createPartition(createPartitionRequest);
      log.debug("{} partition created in glue catalog", partition);
    }
    glueClient.createPartition(createPartitionRequest);
  }

  public void update(Table table,Partition partition) {
    UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
        .withPartitionValueList(transformer.transformPartition(partition).getValues())
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    try {
      glueClient.updatePartition(updatePartitionRequest);
      log.debug("{} partition updated in glue catalog", partition);
    } catch (InvalidInputException e) {
      log.debug("Cleaning up partition manually {} to resolve validation", partition);
      PartitionInput partitionInput = updatePartitionRequest.getPartitionInput();
      updatePartitionRequest.setPartitionInput(cleaner.cleanPartition(partitionInput));
      glueClient.updatePartition(updatePartitionRequest);
      log.debug("{} partition updated in glue catalog", partition);
    }
  }

  public void delete(Table table, Partition partition) {
    DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest()
        .withPartitionValues(transformer.transformPartition(partition).getValues())
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    glueClient.deletePartition(deletePartitionRequest);
    log.debug("{} partition deleted from glue catalog", partition);
  }
}
