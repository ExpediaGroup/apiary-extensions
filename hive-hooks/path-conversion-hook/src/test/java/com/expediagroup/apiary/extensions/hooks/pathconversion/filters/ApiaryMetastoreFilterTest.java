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
package com.expediagroup.apiary.extensions.hooks.pathconversion.filters;

import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableList;

import com.expediagroup.apiary.extensions.hooks.pathconversion.converters.GenericConverter;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryMetastoreFilterTest {

  @Mock private HiveConf hiveConf;
  @Mock private GenericConverter converter;
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
    String mutatedLocation = "targetLocation";

    StorageDescriptor srcSd = new StorageDescriptor();
    srcSd.setLocation("sourceLocation");

    Table sourceTable = new Table();
    sourceTable.setSd(srcSd);

    List<Table> tableList = ImmutableList.of(sourceTable);

    when(converter.convertPath(sourceTable))
        .then((Answer<Boolean>) invocation -> {
          Table inputTable = invocation.getArgument(0);
          inputTable.getSd().setLocation(mutatedLocation);
          return true;
        });

    List<Table> result = hook.filterTables(tableList);
    assertEquals(tableList.size(), result.size());
    assertEquals(mutatedLocation, result.get(0).getSd().getLocation());
  }

  @Test
  public void shouldMutateTable() throws MetaException, NoSuchObjectException {
    String mutatedLocation = "targetLocation";

    StorageDescriptor srcSd = new StorageDescriptor();
    srcSd.setLocation("sourceLocation");

    Table sourceTable = new Table();
    sourceTable.setSd(srcSd);

    when(converter.convertPath(sourceTable))
        .then((Answer<Boolean>) invocation -> {
          Table inputTable = invocation.getArgument(0);
          inputTable.getSd().setLocation(mutatedLocation);
          return true;
        });

    Table result = hook.filterTable(sourceTable);
    assertEquals(mutatedLocation, result.getSd().getLocation());
  }

  @Test
  public void shouldMutatePartitions() throws MetaException, NoSuchObjectException {
    String mutatedLocation = "targetLocation";

    StorageDescriptor srcSd = new StorageDescriptor();
    srcSd.setLocation("sourceLocation");

    Partition sourcePartition = new Partition();
    sourcePartition.setSd(srcSd);

    List<Partition> partitionList = ImmutableList.of(sourcePartition);

    when(converter.convertPath(sourcePartition))
        .then((Answer<Boolean>) invocation -> {
          Partition inputPartition = invocation.getArgument(0);
          inputPartition.getSd().setLocation(mutatedLocation);
          return true;
        });

    List<Partition> result = hook.filterPartitions(partitionList);
    assertEquals(partitionList.size(), result.size());
    assertEquals(mutatedLocation, result.get(0).getSd().getLocation());
  }

  @Test
  public void shouldMutatePartition() throws MetaException, NoSuchObjectException {
    String mutatedLocation = "targetLocation";

    StorageDescriptor srcSd = new StorageDescriptor();
    srcSd.setLocation("sourceLocation");

    Partition sourcePartition = new Partition();
    sourcePartition.setSd(srcSd);

    when(converter.convertPath(sourcePartition))
        .then((Answer<Boolean>) invocation -> {
          Partition inputPartition = invocation.getArgument(0);
          inputPartition.getSd().setLocation(mutatedLocation);
          return true;
        });

    Partition result = hook.filterPartition(sourcePartition);
    assertEquals(mutatedLocation, result.getSd().getLocation());
  }
}
