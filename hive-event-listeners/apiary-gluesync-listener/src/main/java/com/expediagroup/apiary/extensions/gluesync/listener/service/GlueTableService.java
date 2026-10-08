/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.ValidationException;

public class GlueTableService {

  private static final Logger log = LoggerFactory.getLogger(GlueTableService.class);
  // HMS parameters checked before a delete to detect if the table slot was overwritten by a concurrent operation
  private static final String[] IDENTITY_PARAMS = { "transient_lastDdlTime", "metadata_location" };
  public static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";
  public static final String GLUE_SEND_VERSION_ID_ENV = "GLUE_SEND_VERSION_ID";

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();
  private final GluePartitionService gluePartitionService;
  private final boolean sendVersionId;

  public GlueTableService(AWSGlue glueClient, GluePartitionService gluePartitionService, String gluePrefix) {
    this(glueClient, gluePartitionService, gluePrefix, parseSendVersionIdFromEnv());
  }

  public GlueTableService(AWSGlue glueClient, GluePartitionService gluePartitionService, String gluePrefix,
      boolean sendVersionId) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    this.gluePartitionService = gluePartitionService;
    this.sendVersionId = sendVersionId;
    log.debug("ApiaryGlueSync created");
  }

  /**
   * Reads the {@value #GLUE_SEND_VERSION_ID_ENV} environment variable and returns its boolean
   * value. When unset or empty, {@code false} is returned (the feature is off by default). Any
   * value other than {@code "true"}/{@code "false"} (case-insensitive) throws at startup.
   */
  static boolean parseSendVersionIdFromEnv() {
    return parseSendVersionId(System.getenv(GLUE_SEND_VERSION_ID_ENV));
  }

  static boolean parseSendVersionId(String value) {
    if (value == null || value.isEmpty()) {
      return false;
    }
    if ("true".equalsIgnoreCase(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid value for environment variable " + GLUE_SEND_VERSION_ID_ENV
            + ": '" + value + "'. Expected 'true' or 'false'.");
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

  public UpdateOutcome update(Table table) {
    UpdateTableRequest updateTableRequest = new UpdateTableRequest()
        .withSkipArchive(gluePartitionService.shouldSkipArchive(table))
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));

    if (!sendVersionId) {
      doUpdate(updateTableRequest, table);
      return UpdateOutcome.UPDATED;
    }
    return updateWithOptimisticLock(updateTableRequest, table);
  }

  /**
   * Updates the table carrying Glue's optimistic-concurrency {@code versionId}, so AWS records
   * {@code requestParameters.versionId} (the version immediately before this update) on the emitted
   * CloudTrail event. The versionId is best-effort: if it cannot be read, or Glue rejects the write
   * with {@code ConcurrentModificationException} because a concurrent writer bumped the version, the
   * table is updated unconditionally (no {@code versionId}) so the sync still succeeds — matching
   * the listener's last-write-wins behaviour when the feature is off. Only that write loses its
   * {@code versionId}; the outcome records which fallback was taken.
   */
  private UpdateOutcome updateWithOptimisticLock(UpdateTableRequest updateTableRequest, Table table) {
    String versionId = readVersionId(table);
    if (versionId == null) {
      doUpdate(updateTableRequest, table);
      return UpdateOutcome.VERSION_UNAVAILABLE_FALLBACK;
    }
    updateTableRequest.setVersionId(versionId);
    try {
      doUpdate(updateTableRequest, table);
      return UpdateOutcome.UPDATED;
    } catch (ConcurrentModificationException e) {
      log.warn("Concurrent modification updating {}.{} in glue; updating unconditionally without versionId",
          table.getDbName(), table.getTableName());
      updateTableRequest.setVersionId(null);
      doUpdate(updateTableRequest, table);
      return UpdateOutcome.VERSION_CONFLICT_FALLBACK;
    }
  }

  /**
   * Whether to set the optimistic-concurrency {@code versionId} on this table's Glue
   * {@code UpdateTable}. Only non-Iceberg (Hive) tables need it: setting it makes AWS record
   * {@code requestParameters.versionId} on the emitted CloudTrail {@code UpdateTable} event, which
   * consumers of that event use to identify the pre-update table version. Iceberg tables are
   * identified by {@code metadata_location}, so they skip the extra {@code GetTable} and the
   * optimistic-locking overhead.
   */
  /**
   * Issues the Glue UpdateTable, retrying once with cleaned-up comments if Glue rejects the input
   * with {@link ValidationException}/{@link InvalidInputException}. Any {@code versionId} set on
   * the request is preserved across the retry.
   */
  private void doUpdate(UpdateTableRequest updateTableRequest, Table table) {
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

  /**
   * Reads the current Glue {@code VersionId} for the table. Returns {@code null} if it cannot be
   * read (any AWS failure other than the table not existing), so the caller can degrade to an
   * unconditional update rather than failing the sync for a best-effort versionId. An
   * {@link EntityNotFoundException} is allowed to propagate so the caller can create the missing
   * table.
   */
  private String readVersionId(Table table) {
    try {
      return glueClient
          .getTable(new GetTableRequest()
              .withDatabaseName(transformer.glueDbName(table))
              .withName(table.getTableName()))
          .getTable()
          .getVersionId();
    } catch (EntityNotFoundException e) {
      throw e;
    } catch (AmazonClientException e) {
      log.warn("Could not read current Glue versionId for {}.{}; updating without it",
          table.getDbName(), table.getTableName(), e);
      return null;
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

  /**
   * Deletes the Glue table only if {@link #IDENTITY_PARAMS} match the current Glue state, guarding
   * against a race condition in multi-partition Kafka deployments where a concurrent rename can
   * overwrite the table slot before a stale drop event is processed. Safe in direct-HMS deployments
   * because HMS does not update {@code transient_lastDdlTime} on DROP, so the event value always
   * matches the last-synced Glue value.
   *
   * <p>Caveat: if a prior sync failed (e.g. transient Glue error), the params will diverge and the
   * delete will be skipped, leaving an orphan in Glue. An orphan is intentionally preferred over
   * incorrectly deleting a table that has since been overwritten by a rename.
   */
  public DeleteOutcome deleteIfUnchanged(Table table) {
    Map<String, String> hmsParams = table.getParameters() != null ? table.getParameters() : Collections.emptyMap();
    com.amazonaws.services.glue.model.Table glueTable;
    try {
      glueTable = glueClient
          .getTable(new GetTableRequest()
              .withDatabaseName(transformer.glueDbName(table))
              .withName(table.getTableName()))
          .getTable();
    } catch (EntityNotFoundException e) {
      log.info("{}.{} table not found in glue catalog, nothing to delete", table.getDbName(), table.getTableName());
      return DeleteOutcome.NOT_FOUND;
    }
    Map<String, String> glueParams = glueTable.getParameters() != null ? glueTable.getParameters() : Collections.emptyMap();

    for (String key : IDENTITY_PARAMS) {
      if (paramChanged(key, hmsParams, glueParams, table)) {
        return DeleteOutcome.SKIPPED;
      }
    }

    try {
      delete(table);
    } catch (EntityNotFoundException e) {
      log.info("{}.{} table deleted from glue catalog between guard check and delete",
          table.getDbName(), table.getTableName());
      return DeleteOutcome.DELETED_CONCURRENTLY;
    }
    return DeleteOutcome.DELETED;
  }

  private boolean paramChanged(String key, Map<String, String> hmsParams, Map<String, String> glueParams, Table table) {
    String hmsValue = hmsParams.get(key);
    String glueValue = glueParams.get(key);
    if (!Objects.equals(hmsValue, glueValue)) {
      log.warn("Skipping delete of {}.{}: {} changed (expected={}, found={})",
          table.getDbName(), table.getTableName(), key, hmsValue, glueValue);
      return true;
    }
    return false;
  }
}
