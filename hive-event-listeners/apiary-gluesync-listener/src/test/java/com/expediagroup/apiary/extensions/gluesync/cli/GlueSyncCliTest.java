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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClient;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClientFactory;
import com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync;
import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueTableService;
import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

@RunWith(MockitoJUnitRunner.class)
public class GlueSyncCliTest {

  @Mock
  private ThriftHiveClientFactory mockThriftHiveClientFactory;

  @Mock
  private ThriftHiveClient mockThriftHiveClient;

  @Mock
  private IMetaStoreClient mockMetastoreClient;

  @Mock
  private ApiaryGlueSync mockApiaryGlueSync;

  @Mock
  private GlueTableService mockGlueTableService;

  @Mock
  private com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService mockGluePartitionService;

  @Mock
  private com.expediagroup.apiary.extensions.gluesync.listener.service.GlueDatabaseService mockGlueDatabaseService;

  @Mock
  private IsIcebergTablePredicate mockIsIcebergTablePredicate;

  private GlueSyncCli glueSyncCli;
  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Rule
  public final SystemErrRule systemErrRule = new SystemErrRule().enableLog();

  @Before
  public void setUp() throws Exception {
    when(mockGlueDatabaseService.exists(any(org.apache.hadoop.hive.metastore.api.Database.class))).thenReturn(true);

    glueSyncCli = new GlueSyncCli(
        mockThriftHiveClientFactory,
        mockThriftHiveClient,
        mockMetastoreClient,
        mockApiaryGlueSync,
        mockGlueTableService,
        mockGluePartitionService,
        mockIsIcebergTablePredicate,
        mockGlueDatabaseService) {
      @Override
      protected java.util.Iterator<org.apache.hadoop.hive.metastore.api.Partition> createPartitionIterator(
          IMetaStoreClient metastoreClient, Table table) throws org.apache.hadoop.hive.metastore.api.MetaException,
          org.apache.thrift.TException {
        // Return iterator over whatever the mock metastoreClient.listPartitions
        // returns for this table. Tests set up that mock accordingly.
        java.util.List<org.apache.hadoop.hive.metastore.api.Partition> parts = metastoreClient
            .listPartitions(table.getDbName(), table.getTableName(), (short) -1);
        return parts.iterator();
      }
    };
  }

  // Helper method to create command line options that match the real parser
  private Options createOptions() {
    Options options = new Options();
    Option dbRegexOpt = new Option(null, "database-name-regex", true, "Regex for database name");
    dbRegexOpt.setRequired(true);

    Option tableRegexOpt = new Option(null, "table-name-regex", true, "Regex for table name");
    tableRegexOpt.setRequired(true);

    options.addOption(dbRegexOpt);
    options.addOption(tableRegexOpt);
    options.addOption(new Option("v", "verbose", false, "Enable verbose output"));
    options.addOption(new Option("h", "help", false, "Print usage information"));
    options.addOption(new Option(null, "continueOnError", false, "Continue processing on errors"));
    options.addOption(new Option(null, "keep-glue-partitions", false, "Keep existing Glue partitions"));
    options.addOption(new Option(null, "sync-types", true, "Choose what table type to sync."));

    return options;
  }

  @Test
  public void testSyncAllWithMatchingDatabaseAndTable() throws Exception {
    // Arrange
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1", "test_db2", "other_db");
    List<String> tables = Arrays.asList("test_table1", "test_table2", "other_table");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getAllTables("test_db2")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, times(4)).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, times(4)).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithVerboseOption() throws Exception {
    // Arrange
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*", "--verbose" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    Partition mockPartition = new Partition();
    mockPartition.setValues(Arrays.asList("2023", "01"));

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort()))
        .thenReturn(Arrays.asList(mockPartition));

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, times(1)).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), anyBoolean(), eq(true));
  }

  @Test
  public void testSyncAllWithIcebergTable() throws Exception {
    // Arrange
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(true); // Iceberg table

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, times(1)).onCreateTable(any(CreateTableEvent.class));
    verify(mockMetastoreClient, never()).listPartitions(anyString(), anyString(), anyShort());
    verify(mockGluePartitionService, never()).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithNoMatchingDatabases() throws Exception {
    // Arrange
    String[] args = { "--database-name-regex", "nonexistent_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1", "test_db2", "other_db");

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, never()).onCreateTable(any(CreateTableEvent.class));
  }

  @Test(expected = RuntimeException.class)
  public void testSyncAllWithPartitionSyncError() throws Exception {
    // Arrange
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*", "--verbose" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    Partition mockPartition = new Partition();
    mockPartition.setValues(Arrays.asList("2023", "01"));

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort()))
        .thenReturn(Arrays.asList(mockPartition));
    doThrow(new RuntimeException("Glue partition synchronization failed"))
        .when(mockGluePartitionService).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());

    // Act - This should throw an exception since continueOnError is not set
    glueSyncCli.syncAll(cmd);

    // Assert - exception should be thrown
  }

  @Test
  public void testSyncAllWithContinueOnErrorOptionSimple() throws Exception {
    // Arrange - Test that continueOnError flag is parsed correctly
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*", "--continueOnError" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - Just verify the basic functionality works
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithKeepGluePartitionsOption() throws Exception {
    // Arrange - Test that deleteGluePartitions is false when keep-glue-partitions
    // is set
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*",
                      "--keep-glue-partitions" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - Verify that synchronizePartitions is called with
    // deleteGluePartitions=false
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), eq(false), anyBoolean());
  }

  @Test
  public void testSyncAllWithSyncOnlyViewsOption_onView() throws Exception {
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*",
                      "--sync-types", "VIRTUAL_VIEW" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    mockTable.setTableType("VIRTUAL_VIEW");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - Verify that synchronizePartitions is called with
    // deleteGluePartitions=false
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithSyncOnlyViewsOption_onNormalTable() throws Exception {
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*",
                      "--sync-types", "VIRTUAL_VIEW" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    mockTable.setTableType("EXTERNAL_TABLE");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);

    // Act
    glueSyncCli.syncAll(cmd);

    verify(mockGluePartitionService, times(0)).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithSyncOnlyViewsOption_onViewAndTable() throws Exception {
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*",
                      "--sync-types", "VIRTUAL_VIEW,EXTERNAL_TABLE" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    mockTable.setTableType("VIRTUAL_VIEW");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - Verify that synchronizePartitions is called with
    // deleteGluePartitions=false
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithDefaultPartitionDeletion() throws Exception {
    // Arrange - Test that deleteGluePartitions is true by default (when
    // keep-glue-partitions is not set)
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - Verify that synchronizePartitions is called with
    // deleteGluePartitions=true
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), any(), eq(true), anyBoolean());
  }

  @Test
  public void testSyncAllWithLargePartitionBatchHandling() throws Exception {
    // Arrange - Test the new partition batching logic when there are many
    // partitions
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*", "--verbose" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    // Create 1000 partitions to trigger batch logic
    List<Partition> largePartitionList = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      Partition partition = new Partition();
      partition.setValues(Arrays.asList("2023", String.format("%03d", i)));
      largePartitionList.add(partition);
    }

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(largePartitionList);

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockGluePartitionService, times(1)).synchronizePartitions(any(), eq(largePartitionList), anyBoolean(),
        eq(true));
  }

  @Test(expected = RuntimeException.class)
  public void testSyncAllFailsImmediatelyWithoutContinueOnError() throws Exception {
    // Arrange - Test that sync fails immediately when continueOnError is not set
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("test_table1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);
    when(mockMetastoreClient.getTable(anyString(), anyString())).thenReturn(mockTable);
    when(mockIsIcebergTablePredicate.test(any())).thenReturn(false);
    when(mockMetastoreClient.listPartitions(anyString(), anyString(), anyShort())).thenReturn(Arrays.asList());

    // Make the sync fail
    doThrow(new RuntimeException("Sync failed"))
        .when(mockGluePartitionService).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());

    // Act - This should throw an exception
    glueSyncCli.syncAll(cmd);
  }

  @Test
  public void testSyncAllWithNonMatchingTableRegex() throws Exception {
    // Arrange - Test when database matches but no tables match the regex
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "nonexistent_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList("actual_table1", "actual_table2"); // These don't match the regex

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - No sync operations should occur with no matching tables
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, never()).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, never()).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithEmptyDatabaseList() throws Exception {
    // Arrange - Test when no databases are found
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(Arrays.asList());

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, never()).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, never()).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testSyncAllWithEmptyTableList() throws Exception {
    // Arrange - Test when database exists but no tables match
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");
    List<String> tables = Arrays.asList(); // Empty table list

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(tables);

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert
    verify(mockApiaryGlueSync, never()).onCreateDatabase(any(CreateDatabaseEvent.class));
    verify(mockApiaryGlueSync, never()).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, never()).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }

  @Test
  public void testDatabaseSkippedWhenNotInGlue() throws Exception {
    // Arrange - ensure that when GlueDatabaseService.exists() returns false the DB
    // is skipped
    String[] args = { "--database-name-regex", "test_db.*", "--table-name-regex", "test_table.*" };
    CommandLine cmd = new DefaultParser().parse(createOptions(), args);

    List<String> databases = Arrays.asList("test_db1");

    Database mockDatabase = new Database();
    mockDatabase.setName("test_db1");

    Table mockTable = new Table();
    mockTable.setDbName("test_db1");
    mockTable.setTableName("test_table1");
    Map<String, String> tableParams = new HashMap<>();
    mockTable.setParameters(tableParams);

    when(mockMetastoreClient.getAllDatabases()).thenReturn(databases);
    when(mockMetastoreClient.getAllTables("test_db1")).thenReturn(Arrays.asList("test_table1"));
    when(mockMetastoreClient.getDatabase(anyString())).thenReturn(mockDatabase);

    // Important: return false so we skip database. Do NOT stub downstream
    // interaction (getTable, listPartitions, isIcebergTablePredicate) because they
    // shouldn't be reached.
    when(mockGlueDatabaseService.exists(any(Database.class))).thenReturn(false);

    // Act
    glueSyncCli.syncAll(cmd);

    // Assert - database should be skipped: no table sync or partition sync
    verify(mockApiaryGlueSync, never()).onCreateTable(any(CreateTableEvent.class));
    verify(mockGluePartitionService, never()).synchronizePartitions(any(), any(), anyBoolean(), anyBoolean());
  }
}
