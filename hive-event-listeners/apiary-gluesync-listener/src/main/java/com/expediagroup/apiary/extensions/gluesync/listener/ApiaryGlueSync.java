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
package com.expediagroup.apiary.extensions.gluesync.listener;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import com.amazonaws.services.glue.model.EntityNotFoundException;

import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants;
import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueDatabaseService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueTableService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

public class ApiaryGlueSync extends MetaStoreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryGlueSync.class);

  // Simple names of AWS exception types to surface as outcome tags. Walking the class hierarchy
  // from specific to general means subclasses (e.g. OperationTimeoutException) are matched before
  // their superclass (AmazonServiceException) without relying on class literals, which break when
  // the shade plugin relocates com.amazonaws.* to a shaded package at runtime.
  private static final List<String> KNOWN_EXCEPTION_NAMES = Arrays.asList(
      "OperationTimeoutException",
      "InvalidInputException",
      "AmazonServiceException"
  );

  private final AWSGlue glueClient;
  private final GlueDatabaseService glueDatabaseService;
  private final GlueTableService glueTableService;
  private final GluePartitionService gluePartitionService;
  private final IsIcebergTablePredicate isIcebergPredicate;
  private final MetricService metricService;
  private final boolean throwExceptions;

  public ApiaryGlueSync(Configuration config) {
    this(config, false);
  }

  public ApiaryGlueSync(Configuration config, boolean throwExceptions) {
    super(config);
    this.glueClient = AWSGlueClientBuilder.standard().withRegion(System.getenv("AWS_REGION")).build();
    String gluePrefix = System.getenv("GLUE_PREFIX");
    this.glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    this.gluePartitionService = new GluePartitionService(glueClient, gluePrefix);
    this.glueTableService = new GlueTableService(glueClient, gluePartitionService, gluePrefix);
    this.isIcebergPredicate = new IsIcebergTablePredicate();
    this.metricService = new MetricService();
    this.throwExceptions = throwExceptions;
    log.debug("ApiaryGlueSync created");
  }

  /**
   * Just for testing.
   */
  public ApiaryGlueSync(Configuration config, AWSGlue glueClient, String gluePrefix, MetricService metricService,
      boolean throwExceptions) {
    this(config, glueClient, gluePrefix, metricService, throwExceptions, null);
  }

  /**
   * Just for testing. Allows injecting the default {@code SkipArchive} value
   * that would otherwise be read from the
   * {@code GLUE_SKIP_ARCHIVE} environment variable.
   */
  public ApiaryGlueSync(Configuration config, AWSGlue glueClient, String gluePrefix, MetricService metricService,
      boolean throwExceptions, Boolean defaultSkipArchive) {
    super(config);
    this.glueClient = glueClient;
    this.glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    this.gluePartitionService = new GluePartitionService(glueClient, gluePrefix, defaultSkipArchive);
    this.glueTableService = new GlueTableService(glueClient, gluePartitionService, gluePrefix);
    this.isIcebergPredicate = new IsIcebergTablePredicate();
    this.metricService = metricService;
    this.throwExceptions = throwExceptions;
    log.debug("ApiaryGlueSync created");
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.CREATE_DATABASE, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Database database = event.getDatabase();
    try {
      String outcome = createOrUpdateDatabase(database);
      metricService.incrementCounter(MetricConstants.LISTENER_DATABASE_SUCCESS);
      metricService.recordEvent(MetricConstants.CREATE_DATABASE, MetricConstants.RESULT_SUCCESS, outcome);
    } catch (Exception e) {
      log.error("Failed create database {} in glue", database.getName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_DATABASE_FAILURE);
      metricService.recordEvent(MetricConstants.CREATE_DATABASE, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.DROP_DATABASE, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Database database = event.getDatabase();
    try {
      glueDatabaseService.delete(database);
      metricService.incrementCounter(MetricConstants.LISTENER_DATABASE_SUCCESS);
      metricService.recordEvent(MetricConstants.DROP_DATABASE, MetricConstants.RESULT_SUCCESS, "deleted");
    } catch (Exception e) {
      log.error("Failed drop database {} in glue", database.getName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_DATABASE_FAILURE);
      metricService.recordEvent(MetricConstants.DROP_DATABASE, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.CREATE_TABLE, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table table = event.getTable();
    try {
      String outcome = createOrUpdateTable(table);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);
      metricService.recordEvent(MetricConstants.CREATE_TABLE, MetricConstants.RESULT_SUCCESS, outcome);
    } catch (Exception e) {
      log.error("Failed create table {}.{} in glue", table.getDbName(), table.getTableName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_FAILURE);
      metricService.recordEvent(MetricConstants.CREATE_TABLE, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  @Override
  public void onDropTable(DropTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.DROP_TABLE, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table table = event.getTable();
    try {
      glueTableService.delete(table);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);
      metricService.recordEvent(MetricConstants.DROP_TABLE, MetricConstants.RESULT_SUCCESS, "deleted");
    } catch (EntityNotFoundException e) {
      log.info(table + " table doesn't exist in glue catalog");
      metricService.recordEvent(MetricConstants.DROP_TABLE, MetricConstants.RESULT_SUCCESS, "not_found");
      if (throwExceptions) {
        throw wrap(e);
      }
    } catch (Exception e) {
      log.error("Failed drop table {}.{} in glue", table.getDbName(), table.getTableName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_FAILURE);
      metricService.recordEvent(MetricConstants.DROP_TABLE, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.ALTER_TABLE, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table oldTable = event.getOldTable();
    Table newTable = event.getNewTable();
    try {
      // Only Iceberg rename is supported by Glue, for Hive tables we need to delete
      // table and create again
      if (isTableRename(oldTable, newTable) && !isIcebergPredicate.test(oldTable.getParameters())) {
        doRenameOperation(oldTable, newTable);
        return;
      }
      String outcome = updateOrCreateTable(newTable);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);
      metricService.recordEvent(MetricConstants.ALTER_TABLE, MetricConstants.RESULT_SUCCESS, outcome);
    } catch (Exception e) {
      log.error("Failed alter table {}.{} in glue", oldTable.getDbName(), oldTable.getTableName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_TABLE_FAILURE);
      metricService.recordEvent(MetricConstants.ALTER_TABLE, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  private String createOrUpdateDatabase(Database database) {
    try {
      glueDatabaseService.create(database);
      return MetricConstants.OUTCOME_CREATED;
    } catch (AlreadyExistsException e) {
      log.info("{} database already exists in glue, updating....", database.getName());
      glueDatabaseService.update(database);
      return MetricConstants.OUTCOME_UPDATED;
    }
  }

  private String createOrUpdateTable(Table table) {
    try {
      glueTableService.create(table);
      return MetricConstants.OUTCOME_CREATED;
    } catch (AlreadyExistsException e) {
      log.info("{} table already exists in glue, updating....", table.getTableName());
      glueTableService.update(table);
      return MetricConstants.OUTCOME_UPDATED;
    }
  }

  private String updateOrCreateTable(Table table) {
    try {
      glueTableService.update(table);
      return MetricConstants.OUTCOME_UPDATED;
    } catch (EntityNotFoundException e) {
      log.info("{} table doesn't exist in glue, creating....", table.getTableName());
      glueTableService.create(table);
      return MetricConstants.OUTCOME_CREATED;
    }
  }

  private String createOrUpdatePartition(Table table, Partition partition) {
    try {
      gluePartitionService.create(table, partition);
      return MetricConstants.OUTCOME_CREATED;
    } catch (AlreadyExistsException e) {
      log.info("{} partition already exists in glue, updating....", partition);
      gluePartitionService.update(table, partition);
      return MetricConstants.OUTCOME_UPDATED;
    }
  }

  private String updateOrCreatePartition(Table table, Partition partition) {
    try {
      gluePartitionService.update(table, partition);
      return MetricConstants.OUTCOME_UPDATED;
    } catch (EntityNotFoundException e) {
      log.info("{} partition doesn't exist in glue, creating....", partition);
      gluePartitionService.create(table, partition);
      return MetricConstants.OUTCOME_CREATED;
    }
  }

  private void doRenameOperation(Table oldTable, Table newTable) {
    log.info("{} glue table rename detected to {}", oldTable.getTableName(), newTable.getTableName());
    long startTime = System.currentTimeMillis();
    glueTableService.create(newTable);
    gluePartitionService.copyPartitions(newTable, gluePartitionService.getPartitions(oldTable));
    glueTableService.delete(oldTable);
    metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);
    metricService.recordEvent(MetricConstants.ALTER_TABLE, MetricConstants.RESULT_SUCCESS, "renamed");
    long duration = System.currentTimeMillis() - startTime;
    metricService.recordDuration(MetricConstants.LISTENER_TABLE_RENAME_DURATION, duration);
    log.info("{} glue table rename to {} finished in {}ms", oldTable.getTableName(), newTable.getTableName(), duration);
  }

  private boolean isTableRename(Table oldTable, Table newTable) {
    return !oldTable.getTableName().equals(newTable.getTableName());
  }

  @Override
  public void onAddPartition(AddPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.ADD_PARTITION, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        String outcome = createOrUpdatePartition(table, partition);
        metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_SUCCESS);
        metricService.recordEvent(MetricConstants.ADD_PARTITION, MetricConstants.RESULT_SUCCESS, outcome);
      } catch (Exception e) {
        log.error("Failed add partition on table {}.{} in glue", table.getDbName(), table.getTableName(), e);
        metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_FAILURE);
        metricService.recordEvent(MetricConstants.ADD_PARTITION, MetricConstants.RESULT_FAILURE, toOutcome(e));
        if (throwExceptions) {
          throw wrap(e);
        }
      }
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.DROP_PARTITION, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        gluePartitionService.delete(table, partition);
        metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_SUCCESS);
        metricService.recordEvent(MetricConstants.DROP_PARTITION, MetricConstants.RESULT_SUCCESS, "deleted");
      } catch (EntityNotFoundException e) {
        log.info("{} partition doesn't exist in glue catalog", partition);
        metricService.recordEvent(MetricConstants.DROP_PARTITION, MetricConstants.RESULT_SUCCESS, "not_found");
        if (throwExceptions) {
          throw wrap(e);
        }
      } catch (Exception e) {
        log.error("Failed drop partition on table {}.{} in glue", table.getDbName(), table.getTableName(), e);
        metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_FAILURE);
        metricService.recordEvent(MetricConstants.DROP_PARTITION, MetricConstants.RESULT_FAILURE, toOutcome(e));
        if (throwExceptions) {
          throw wrap(e);
        }
      }
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    if (!event.getStatus()) {
      metricService.recordEvent(MetricConstants.ALTER_PARTITION, MetricConstants.RESULT_IGNORED, "ignored");
      return;
    }
    Table table = event.getTable();
    Partition partition = event.getNewPartition();
    try {
      String outcome = updateOrCreatePartition(table, partition);
      metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_SUCCESS);
      metricService.recordEvent(MetricConstants.ALTER_PARTITION, MetricConstants.RESULT_SUCCESS, outcome);
    } catch (Exception e) {
      log.error("Failed alter partition on table {}.{} in glue", table.getDbName(), table.getTableName(), e);
      metricService.incrementCounter(MetricConstants.LISTENER_PARTITION_FAILURE);
      metricService.recordEvent(MetricConstants.ALTER_PARTITION, MetricConstants.RESULT_FAILURE, toOutcome(e));
      if (throwExceptions) {
        throw wrap(e);
      }
    }
  }

  private static String toOutcome(Exception e) {
    Class<?> type = e.getClass();
    while (type != null) {
      if (KNOWN_EXCEPTION_NAMES.contains(type.getSimpleName())) {
        return type.getSimpleName();
      }
      type = type.getSuperclass();
    }
    return "other";
  }

  /*
   * Helper method to wrap random exceptions into a MetaException along with their
   * stack traces so we don't lose info. This is really only needed in the CLI
   * because I want to be able to see if it failed in the CLI.
   */
  private MetaException wrap(Throwable t) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    String msg = t.getClass().getName() + ": " + t.getMessage() + "\n" + sw.toString();
    return new MetaException(msg);
  }
}
