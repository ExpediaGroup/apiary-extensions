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

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.ValidationException;

/*
//TODO: Should we just 'always clean' partitions and tables? Instead of doing it conditionally on failure only?
//Reasoning: Non-unicode characters seem to be universally bad... I can't think of when we need them and it would
//simply the code. Right now it's only spotty where we do it anyway.
*/

public class GlueTableService {
  private static final Logger log = LoggerFactory.getLogger(GlueTableService.class);
  public static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();
  private final GluePartitionService gluePartitionService;

  public GlueTableService(AWSGlue glueClient, GluePartitionService gluePartitionService, String gluePrefix) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    this.gluePartitionService = gluePartitionService;
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table) {
    CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));
    try {
      glueClient.createTable(createTableRequest);
      log.debug(table + " table created in glue catalog");
    } catch (ValidationException | InvalidInputException e) {
      TableInput tableInput = createTableRequest.getTableInput();
      createTableRequest.setTableInput(cleanUpTable(tableInput));
      glueClient.createTable(createTableRequest);
      log.debug(table + " table updated in glue catalog");
    }
  }

  public void update(Table table) {
    boolean skipArchive = gluePartitionService.shouldSkipArchive(table);

    UpdateTableRequest updateTableRequest = new UpdateTableRequest()
        .withSkipArchive(skipArchive)
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));
    try {
      glueClient.updateTable(updateTableRequest);
      log.debug(table + " table updated in glue catalog");
    } catch (ValidationException | InvalidInputException e) {
      TableInput tableInput = updateTableRequest.getTableInput();
      updateTableRequest.setTableInput(cleanUpTable(tableInput));
      glueClient.updateTable(updateTableRequest);
      log.debug(table + " table updated in glue catalog");
    }
  }

  private TableInput cleanUpTable(TableInput table) {
    log.debug("Cleaning up table comments manually on {} to resolve validation", table);
    long startTime = System.currentTimeMillis();
    TableInput result = cleaner.cleanTable(table);
    long duration = System.currentTimeMillis() - startTime;
    log.debug("Clean up table comments operation on {} finished in {}ms", table, duration);
    return result;
  }

  public void delete(Table table) {
    DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
        .withName(table.getTableName())
        .withDatabaseName(transformer.glueDbName(table));
    glueClient.deleteTable(deleteTableRequest);
    log.debug(table + " table deleted from glue catalog");
  }
}
