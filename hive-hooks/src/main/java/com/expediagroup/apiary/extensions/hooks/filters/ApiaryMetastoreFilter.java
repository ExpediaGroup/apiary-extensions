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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.hooks.config.Configuration;
import com.expediagroup.apiary.extensions.hooks.converters.GenericConverter;
import com.expediagroup.apiary.extensions.hooks.pathconversion.converters.PathConverter;

public class ApiaryMetastoreFilter implements MetaStoreFilterHook {

  private final Configuration configuration;
  private final GenericConverter converter;

  public ApiaryMetastoreFilter(HiveConf conf) {
    configuration = new Configuration(conf);
    converter = new PathConverter(configuration);
  }

  @VisibleForTesting
  public ApiaryMetastoreFilter(HiveConf conf, GenericConverter schemeConverter) {
    configuration = new Configuration(conf);
    converter = schemeConverter;
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) {
    return dbList;
  }

  @Override
  public Database filterDatabase(Database dataBase) {
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) {
    return tableList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList) {
    return partitionSpecList;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName, List<String> partitionNames) {
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index) {
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName, List<String> indexList) {
    return indexList;
  }

  @Override
  public List<Index> filterIndexes(List<Index> indexList) {
    return indexList;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) {
    tableList.stream().forEach(converter::convertPath);
    return tableList;
  }

  @Override
  public Table filterTable(Table table) {
    converter.convertPath(table);
    return table;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) {
    partitionList.stream().forEach(converter::convertPath);
    return partitionList;
  }

  @Override
  public Partition filterPartition(Partition partition) {
    converter.convertPath(partition);
    return partition;
  }
}
