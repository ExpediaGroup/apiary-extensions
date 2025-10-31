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

package com.expediagroup.apiary.extensions.gluesync.cli;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClient;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClientFactory;
import com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueDatabaseService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueTableService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

import com.hotels.hcommon.hive.metastore.iterator.PartitionIterator;

public class GlueSyncCli {

  private static final Logger logger = LoggerFactory.getLogger(GlueSyncCli.class);

  private static final String THRIFT_CONNECTION_URI = System.getenv("THRIFT_CONNECTION_URI");
  private static final String THRIFT_CONNECTION_TIMEOUT = "20000";
  private static final short DEFAULT_PARTITION_BATCH_SIZE = 1000;

  private final ThriftHiveClientFactory thriftHiveClientFactory;
  private final ThriftHiveClient thriftHiveClient;
  private final IMetaStoreClient metastoreClient;
  private final ApiaryGlueSync apiaryGlueSync;
  private final GluePartitionService gluePartitionService;
  private final GlueDatabaseService glueDatabaseService;
  private IsIcebergTablePredicate isIcebergTablePredicate;

  public GlueSyncCli() {
    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.setRequestTimeout(600000);
    AWSGlue glueClient = AWSGlueClientBuilder.standard()
        .withRegion(System.getenv("AWS_REGION"))
        .withClientConfiguration(clientConfig)
        .build();
    this.thriftHiveClientFactory = new ThriftHiveClientFactory();
    thriftHiveClient = thriftHiveClientFactory.newInstance(THRIFT_CONNECTION_URI, THRIFT_CONNECTION_TIMEOUT);
    metastoreClient = thriftHiveClient.getMetaStoreClient();
    Configuration config = new Configuration();
    this.apiaryGlueSync = new ApiaryGlueSync(config, true);
    this.isIcebergTablePredicate = new IsIcebergTablePredicate();
    String gluePrefix = System.getenv("GLUE_PREFIX");
    this.gluePartitionService = new GluePartitionService(glueClient, gluePrefix);
    this.glueDatabaseService = new GlueDatabaseService(glueClient, gluePrefix);
  }

  // For testing
  public GlueSyncCli(ThriftHiveClientFactory thriftHiveClientFactory,
      ThriftHiveClient thriftHiveClient,
      IMetaStoreClient metastoreClient,
      ApiaryGlueSync apiaryGlueSync,
      GlueTableService glueTableService,
      GluePartitionService gluePartitionService,
      IsIcebergTablePredicate isIcebergTablePredicate,
      GlueDatabaseService glueDatabaseService) {
    this.thriftHiveClientFactory = thriftHiveClientFactory;
    this.thriftHiveClient = thriftHiveClient;
    this.metastoreClient = metastoreClient;
    this.apiaryGlueSync = apiaryGlueSync;
    this.isIcebergTablePredicate = isIcebergTablePredicate;
    this.gluePartitionService = gluePartitionService;
    this.glueDatabaseService = glueDatabaseService;
  }

  public void syncAll(CommandLine cmd) throws TException {
    logger.debug("Starting GlueSync operation");

    String dbRegex = cmd.getOptionValue("database-name-regex");
    String tableRegex = cmd.getOptionValue("table-name-regex");
    boolean verbose = cmd.hasOption("verbose");

    logger.debug("Sync parameters: dbRegex={}, tableRegex={}, verbose={}", dbRegex, tableRegex, verbose);

    boolean continueOnError = cmd.hasOption("continueOnError");
    boolean deleteGluePartitions = !cmd.hasOption("keep-glue-partitions");
    boolean syncOnlyViews = cmd.hasOption("sync-only-views");

    logger.debug("Additional parameters: continueOnError={}, deleteGluePartitions={}, syncOnlyViews={}",
        continueOnError, deleteGluePartitions, syncOnlyViews);

    boolean hadError = false;
    for (String dbName : metastoreClient.getAllDatabases()) {
      if (dbName.matches(dbRegex)) {
        logger.debug("Processing database: {}", dbName);
        for (String tableName : metastoreClient.getAllTables(dbName)) {
          if (tableName.matches(tableRegex)) {
            try {
              logger.info("Syncing table: {} in database: {}", tableName, dbName);
              syncTable(dbName, tableName, deleteGluePartitions, syncOnlyViews, verbose);
            } catch (Exception e) {
              hadError = true;
              logger.error("Error syncing table: {} in database: {}: {}", tableName, dbName, e.getMessage());
              if (!continueOnError) {
                throw new RuntimeException("Error during sync operation", e);
              }
            }
          }
        }
      }
    }
    if (hadError) {
      logger.warn("Sync operation completed with errors.");
    } else {
      logger.info("Sync operation completed successfully.");
    }
  }

  private void syncTable(String dbName, String tableName, boolean deleteGluePartitions, boolean syncOnlyViews,
      boolean verbose) throws TException {
    Database database = metastoreClient.getDatabase(dbName);

    if (!glueDatabaseService.exists(database)) {
      logger.info("Database {} does not exist in Glue catalog, skipping the whole db...", database.getName());
      return;
    }

    Table table = metastoreClient.getTable(dbName, tableName);

    if (syncOnlyViews) {
      String type = table.getTableType();
      if (type != null && !type.equalsIgnoreCase("VIRTUAL_VIEW")) {
        logger.info("Table {}.{} is not a view, skipping as syncOnlyViews flag is active", dbName, tableName);
        return;
      }
    }

    CreateTableEvent createTableEvent = new CreateTableEvent(table, true, null);
    apiaryGlueSync.onCreateTable(createTableEvent);

    /* Not sure if I need this or not */
    if (!isIcebergTablePredicate.test(table.getParameters())) {
      List<Partition> partitions = getPartitions(table);
      gluePartitionService.synchronizePartitions(table, partitions, deleteGluePartitions, verbose);
    }

    logger.info("Successfully synced database: {} and table: {} to Glue", dbName, tableName);
  }

  private List<Partition> getPartitions(Table table) throws MetaException, TException {
    List<Partition> partitions = new ArrayList<>();
    Iterator<Partition> partitionIterator;
    partitionIterator = createPartitionIterator(metastoreClient, table);
    while (partitionIterator.hasNext()) {
      partitions.add(partitionIterator.next());
    }
    return partitions;
  }

  protected Iterator<Partition> createPartitionIterator(IMetaStoreClient metastoreClient, Table table)
      throws org.apache.hadoop.hive.metastore.api.MetaException, org.apache.thrift.TException {
    return new PartitionIterator(metastoreClient, table, DEFAULT_PARTITION_BATCH_SIZE);
  }
}
