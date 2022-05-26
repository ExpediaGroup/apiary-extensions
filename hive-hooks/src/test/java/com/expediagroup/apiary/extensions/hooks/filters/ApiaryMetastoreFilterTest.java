/**
 * Copyright (C) 2018-2021 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.hooks.filters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.expediagroup.apiary.extensions.hooks.converters.GenericConverter;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryMetastoreFilterTest {

  @Mock
  private HiveConf hiveConf;
  @Mock
  private GenericConverter converter;
  private MetaStoreFilterHook hook;

  @Before
  public void init() {
    Properties mockProps = new Properties();
    when(hiveConf.getAllProperties()).thenReturn(mockProps);
    hook = new ApiaryMetastoreFilter(hiveConf, converter);
  }

  @Test
  public void shouldNotMutateDatabases() throws MetaException {
    List<String> dbNames = ImmutableList.of("test");
    List<String> result = hook.filterDatabases(dbNames);
    assertEquals(dbNames, result);
  }

  @Test
  public void shouldNotMutateDatabase() throws MetaException, NoSuchObjectException {
    Database db = new Database();
    Database result = hook.filterDatabase(db);
    assertEquals(db, result);
  }

  @Test
  public void shouldNotMutateTableNames() throws MetaException {
    List<String> tableNames = ImmutableList.of("test");
    List<String> result = hook.filterTableNames("db", tableNames);
    assertEquals(tableNames, result);
  }

  @Test
  public void shouldNotMutatePartitionSpec() throws MetaException {
    List<PartitionSpec> partitionSpecs = ImmutableList.of(new PartitionSpec());
    List<PartitionSpec> result = hook.filterPartitionSpecs(partitionSpecs);
    assertEquals(partitionSpecs, result);
  }

  @Test
  public void shouldNotMutatePartitionNames() throws MetaException {
    List<String> partitionNames = ImmutableList.of("test");
    List<String> result = hook.filterPartitionNames("db", "tbl", partitionNames);
    assertEquals(partitionNames, result);
  }

  @Test
  public void shouldNotMutateIndex() throws MetaException, NoSuchObjectException {
    Index index = new Index();
    Index result = hook.filterIndex(index);
    assertEquals(index, result);
  }

  @Test
  public void shouldNotMutateIndexNames() throws MetaException {
    List<String> indexNames = ImmutableList.of("test");
    List<String> result = hook.filterIndexNames("db", "tbl", indexNames);
    assertEquals(indexNames, result);
  }

  @Test
  public void shouldNotMutateIndexes() throws MetaException {
    List<Index> indexList = ImmutableList.of(new Index());
    List<Index> result = hook.filterIndexes(indexList);
    assertEquals(indexList, result);
  }

  @Test
  public void shouldMutateTables() throws MetaException {
    List<Table> tableList = ImmutableList.of(createMockTable("tableOne"), createMockTable("tableTwo"));

    List<Table> result = hook.filterTables(tableList);
    assertEquals(result.size(), tableList.size());
    tableList.stream().forEach(table -> verify(converter).convertTable(table));
  }

  @Test
  public void shouldMutateTable() throws MetaException, NoSuchObjectException {
    Table sourceTable = createMockTable("mockTable");
    Table result = hook.filterTable(sourceTable);
    assertEquals(result, sourceTable);
    verify(converter).convertTable(sourceTable);
  }

  @Test
  public void shouldMutateTablesThrowingExceptionsStillReturnsAll() throws MetaException, NoSuchObjectException {
    Table t1 = createMockTable("tableOne");
    Table t2 = createMockTable("tableTwo");
    List<Table> tableList = ImmutableList.of(t1, t2);
    when(converter.convertTable(t1)).thenThrow(new RuntimeException("anything can happen"));

    List<Table> result = hook.filterTables(tableList);
    assertThat(result.size(), is(2));
    verify(converter).convertTable(t2);
  }

  @Test
  public void shouldMutatePartitions() throws MetaException, NoSuchObjectException {
    List<Partition> partitionList = ImmutableList.of(createMockPartition("tableOne"), createMockPartition("tableTwo"));

    List<Partition> result = hook.filterPartitions(partitionList);
    assertEquals(result.size(), partitionList.size());
    partitionList.stream().forEach(partition -> verify(converter).convertPartition(partition));
  }

  @Test
  public void shouldMutatePartition() throws MetaException, NoSuchObjectException {
    Partition sourcePartition = createMockPartition("mockPartition");
    Partition result = hook.filterPartition(sourcePartition);
    assertEquals(sourcePartition, result);
    verify(converter).convertPartition(sourcePartition);
  }

  @Test
  public void shouldMutatePartitionsThrowingExceptionsStillReturnsAll() throws MetaException, NoSuchObjectException {
    Partition p1 = createMockPartition("tableOne");
    Partition p2 = createMockPartition("tableTwo");
    List<Partition> partitionList = ImmutableList.of(p1, p2);
    when(converter.convertPartition(p1)).thenThrow(new RuntimeException("anything can happen"));

    List<Partition> result = hook.filterPartitions(partitionList);
    assertThat(result.size(), is(2));
    verify(converter).convertPartition(p2);
  }

  private Table createMockTable(String tableName) {
    Table table = new Table();
    table.setTableName(tableName);
    return table;
  }

  private Partition createMockPartition(String tableName) {
    Partition partition = new Partition();
    partition.setTableName(tableName);
    return partition;
  }
}
