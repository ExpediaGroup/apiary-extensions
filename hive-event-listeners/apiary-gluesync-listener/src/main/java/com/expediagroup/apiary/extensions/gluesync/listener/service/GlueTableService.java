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

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.ValidationException;

public class GlueTableService {

  /**
   * Outcome of a {@link #deleteIfUnchanged} call, used by callers to record the appropriate metric.
   * <ul>
   *   <li>{@code DELETED} — the Glue table was deleted.</li>
   *   <li>{@code SKIPPED} — the delete was suppressed because identity params diverged, indicating
   *       the Glue slot was overwritten by a concurrent operation.</li>
   *   <li>{@code NOT_FOUND} — the table did not exist in Glue when checked; nothing to delete.</li>
   *   <li>{@code DELETED_CONCURRENTLY} — the table existed at guard-check time but was deleted by a
   *       concurrent operation before the delete call completed (TOCTOU race).</li>
   * </ul>
   */
  public enum DeleteOutcome {
    DELETED("deleted"),
    SKIPPED("delete_skipped"),
    NOT_FOUND("not_found"),
    DELETED_CONCURRENTLY("concurrent_delete");

    private final String metricOutcome;

    DeleteOutcome(String metricOutcome) {
      this.metricOutcome = metricOutcome;
    }

    public String metricOutcome() {
      return metricOutcome;
    }
  }

  /**
   * Outcome of an {@link #update} call.
   * <ul>
   *   <li>{@code UPDATED} — the Glue table was updated (the normal path).</li>
   *   <li>{@code VERSION_CONFLICT_FALLBACK} — optimistic-locking retries were exhausted, so the
   *       update was retried unconditionally (without a versionId); the sync still succeeded but
   *       that UpdateTable event does not carry a versionId.</li>
   * </ul>
   */
  public enum UpdateOutcome {
    UPDATED("updated"),
    VERSION_CONFLICT_FALLBACK("version_conflict_fallback");

    private final String metricOutcome;

    UpdateOutcome(String metricOutcome) {
      this.metricOutcome = metricOutcome;
    }

    public String metricOutcome() {
      return metricOutcome;
    }
  }

  private static final Logger log = LoggerFactory.getLogger(GlueTableService.class);
  // HMS parameters checked before a delete to detect if the table slot was overwritten by a concurrent operation
  private static final String[] IDENTITY_PARAMS = { "transient_lastDdlTime", "metadata_location" };
  public static final String APIARY_GLUESYNC_SKIP_ARCHIVE_TABLE_PARAM = "apiary.gluesync.skipArchive";
  // Bounded re-fetch-and-retry on Glue optimistic-locking conflicts before falling back to an
  // unconditional update.
  private static final int MAX_VERSION_ID_ATTEMPTS = 3;

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();
  private final GluePartitionService gluePartitionService;
  private final IsIcebergTablePredicate isIcebergPredicate = new IsIcebergTablePredicate();

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

  public UpdateOutcome update(Table table) {
    boolean skipArchive = gluePartitionService.shouldSkipArchive(table);

    UpdateTableRequest updateTableRequest = new UpdateTableRequest()
        .withSkipArchive(skipArchive)
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));

    if (!shouldSendVersionId(table)) {
      doUpdate(updateTableRequest, table);
      return UpdateOutcome.UPDATED;
    }
    return updateWithOptimisticLock(updateTableRequest, table);
  }

  /**
   * Updates the table while carrying Glue's optimistic-concurrency {@code versionId}, so AWS records
   * {@code requestParameters.versionId} (the version immediately before this update) on the emitted
   * CloudTrail event. Glue enforces the {@code versionId} as a compare-and-swap, so a concurrent
   * writer fails the write with {@code ConcurrentModificationException}; that is retried against a
   * freshly-read version. If the retries are exhausted, the table is updated unconditionally (no
   * {@code versionId}) so the sync still succeeds for a contended table — that write just does not
   * carry a {@code versionId}.
   */
  private UpdateOutcome updateWithOptimisticLock(UpdateTableRequest updateTableRequest, Table table) {
    log.debug("Updating {}.{} in glue with optimistic locking (versionId)", table.getDbName(), table.getTableName());
    boolean applied = OptimisticUpdateRetry.retryOnConflict(
        MAX_VERSION_ID_ATTEMPTS,
        attemptNo -> log.warn("Concurrent modification updating {}.{} in glue (attempt {}/{}); refreshing versionId",
            table.getDbName(), table.getTableName(), attemptNo, MAX_VERSION_ID_ATTEMPTS),
        () -> {
          updateTableRequest.setVersionId(currentVersionId(table));
          doUpdate(updateTableRequest, table);
        },
        () -> {
          log.warn("Exhausted {} versionId conflict retries updating {}.{} in glue; "
              + "falling back to an unconditional update without versionId",
              MAX_VERSION_ID_ATTEMPTS, table.getDbName(), table.getTableName());
          updateTableRequest.setVersionId(null);
          doUpdate(updateTableRequest, table);
        });
    return applied ? UpdateOutcome.UPDATED : UpdateOutcome.VERSION_CONFLICT_FALLBACK;
  }

  /**
   * Whether to set the optimistic-concurrency {@code versionId} on this table's Glue
   * {@code UpdateTable}. Only non-Iceberg (Hive) tables need it: setting it makes AWS record
   * {@code requestParameters.versionId} on the emitted CloudTrail {@code UpdateTable} event, which
   * consumers of that event use to identify the pre-update table version. Iceberg tables are
   * identified by {@code metadata_location}, so they skip the extra {@code GetTable} and the
   * optimistic-locking overhead.
   */
  private boolean shouldSendVersionId(Table table) {
    return gluePartitionService.isSendVersionId() && !isIcebergPredicate.test(table.getParameters());
  }

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

  private String currentVersionId(Table table) {
    return glueClient
        .getTable(new GetTableRequest()
            .withDatabaseName(transformer.glueDbName(table))
            .withName(table.getTableName()))
        .getTable()
        .getVersionId();
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
