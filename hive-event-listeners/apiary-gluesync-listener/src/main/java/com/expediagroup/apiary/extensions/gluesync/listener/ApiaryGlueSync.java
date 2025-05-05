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

import java.util.Iterator;

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
import com.amazonaws.services.glue.model.EntityNotFoundException;

import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueDatabaseService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueTableService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

public class ApiaryGlueSync extends MetaStoreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryGlueSync.class);
  private final IsIcebergTablePredicate isIcebergPredicate = new IsIcebergTablePredicate();

  private final AWSGlue glueClient;
  private final GlueDatabaseService glueDatabaseService;
  private final GlueTableService glueTableService;
  private final GluePartitionService gluePartitionService;

  public ApiaryGlueSync(Configuration config) {
    super(config);
    glueClient = AWSGlueClientBuilder.standard().withRegion(System.getenv("AWS_REGION")).build();
    String gluePrefix = System.getenv("GLUE_PREFIX");
    glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    glueTableService = new GlueTableService(glueClient, gluePrefix);
    gluePartitionService = new GluePartitionService(glueClient, gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public ApiaryGlueSync(Configuration config, AWSGlue glueClient, String gluePrefix) {
    super(config);
    this.glueClient = glueClient;
    glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
    glueTableService = new GlueTableService(glueClient, gluePrefix);
    gluePartitionService = new GluePartitionService(glueClient, gluePrefix);
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
      glueTableService.create(table);
    } catch (AlreadyExistsException e) {
      log.info(table + " table already exists in glue, updating....");
      glueTableService.update(table);
    }
  }

  @Override
  public void onDropTable(DropTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    try {
      glueTableService.delete(table);
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
      // Only Iceberg rename is supported by Glue, for Hive tables we need to delete table and create again
      if (isTableRename(oldTable, newTable) && !isIcebergPredicate.test(oldTable.getParameters())) {
        doRenameOperation(oldTable, newTable);
        return;
      }
      glueTableService.update(newTable);
    } catch (EntityNotFoundException e) {
      log.info(newTable + " table doesn't exist in glue, creating....");
      glueTableService.create(newTable);
    }
  }

  private void doRenameOperation(Table oldTable, Table newTable) {
    log.info("{} glue table rename detected to {}", oldTable.getTableName(), newTable.getTableName());
    long startTime = System.currentTimeMillis();
    glueTableService.create(newTable);
    glueTableService.copyPartitions(newTable, glueTableService.getPartitions(oldTable));
    glueTableService.delete(oldTable);
    long duration = System.currentTimeMillis() - startTime;
    log.info("{} glue table rename to {} finised in {}ms", oldTable.getTableName(), newTable.getTableName(), duration);
  }

  private boolean isTableRename(Table oldTable, Table newTable) {
    return !oldTable.getTableName().equals(newTable.getTableName());
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
        gluePartitionService.create(table, partition);
      } catch (AlreadyExistsException e) {
        gluePartitionService.update(table, partition);
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
        gluePartitionService.delete(table, partition);
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
      gluePartitionService.update(table, partition);
    } catch (EntityNotFoundException e) {
      gluePartitionService.create(table, partition);
    }
  }
}
