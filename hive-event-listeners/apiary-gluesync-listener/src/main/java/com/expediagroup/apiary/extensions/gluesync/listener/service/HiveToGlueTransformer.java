/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;

public class HiveToGlueTransformer {
  private static final Logger log = LoggerFactory.getLogger(HiveToGlueTransformer.class);
  public static final String MANAGED_BY_GLUESYNC_KEY = "managed-by";
  public static final String MANAGED_BY_GLUESYNC_VALUE = "apiary-glue-sync";

  private final StringCleaner stringCleaner = new StringCleaner();

  private final String gluePrefix;

  public HiveToGlueTransformer(String gluePrefix) {
    this.gluePrefix = gluePrefix;
  }

  public DatabaseInput transformDatabase(Database database) {
    Map<String, String> params = database.getParameters();
    if (params == null) {
      params = new HashMap<>();
    }
    params.put(MANAGED_BY_GLUESYNC_KEY, MANAGED_BY_GLUESYNC_VALUE);
    return new DatabaseInput().withName(glueDbName(database.getName()))
        .withParameters(params)
        .withDescription(database.getDescription())
        .withLocationUri(database.getLocationUri());
  }

  public TableInput transformTable(final Table table) {
    final Date date = convertMillisToTableDate(table.getLastAccessTime());

    List<Column> partitionKeys = extractColumns(table.getPartitionKeys());

    final org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = table.getSd();
    final List<Column> columns = extractColumns(storageDescriptor.getCols());

    final SerDeInfo glueSerde = new SerDeInfo()
        .withName(storageDescriptor.getSerdeInfo().getName())
        .withParameters(storageDescriptor.getSerdeInfo().getParameters())
        .withSerializationLibrary(storageDescriptor.getSerdeInfo().getSerializationLib());

    final List<Order> sortOrders = extractSortOrders(storageDescriptor.getSortCols());

    final StorageDescriptor sd = new StorageDescriptor()
        .withBucketColumns(storageDescriptor.getBucketCols())
        .withColumns(columns)
        .withCompressed(storageDescriptor.isCompressed())
        .withInputFormat(storageDescriptor.getInputFormat())
        .withLocation(storageDescriptor.getLocation())
        .withNumberOfBuckets(storageDescriptor.getNumBuckets())
        .withOutputFormat(storageDescriptor.getOutputFormat())
        .withParameters(storageDescriptor.getParameters())
        .withSerdeInfo(glueSerde)
        .withSortColumns(sortOrders)
        .withStoredAsSubDirectories(storageDescriptor.isStoredAsSubDirectories());

    return new TableInput()
        .withName(stringCleaner.clean(table.getTableName()))
        .withLastAccessTime(date)
        .withOwner(table.getOwner())
        .withParameters(table.getParameters())
        .withPartitionKeys(partitionKeys)
        .withRetention(table.getRetention())
        .withStorageDescriptor(sd)
        .withTableType(table.getTableType());
  }

  public PartitionInput transformPartition(final Partition partition) {
    final Date date = convertMillisToTableDate(partition.getLastAccessTime());

    final org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = partition.getSd();
    final Collection<Column> columns = extractColumns(storageDescriptor.getCols());

    final SerDeInfo glueSerde = new SerDeInfo()
        .withName(stringCleaner.clean(storageDescriptor.getSerdeInfo().getName()))
        .withParameters(storageDescriptor.getSerdeInfo().getParameters())
        .withSerializationLibrary(storageDescriptor.getSerdeInfo().getSerializationLib());

    final List<Order> sortOrders = extractSortOrders(storageDescriptor.getSortCols());

    final StorageDescriptor sd = new StorageDescriptor()
        .withBucketColumns(storageDescriptor.getBucketCols())
        .withColumns(columns)
        .withCompressed(storageDescriptor.isCompressed())
        .withInputFormat(storageDescriptor.getInputFormat())
        .withLocation(storageDescriptor.getLocation())
        .withNumberOfBuckets(storageDescriptor.getNumBuckets())
        .withOutputFormat(storageDescriptor.getOutputFormat())
        .withParameters(storageDescriptor.getParameters())
        .withSerdeInfo(glueSerde)
        .withSortColumns(sortOrders)
        .withStoredAsSubDirectories(storageDescriptor.isStoredAsSubDirectories());

    List<String> partitionValues = partition.getValues().stream().map(stringCleaner::clean).collect(Collectors.toList());

    return new PartitionInput()
        .withLastAccessTime(date)
        .withParameters(partition.getParameters())
        .withStorageDescriptor(sd)
        .withValues(partitionValues);
  }

  public String glueDbName(String dbName) {
    return (gluePrefix == null) ? dbName : gluePrefix + dbName;
  }

  public String glueDbName(Table table) {
    return glueDbName(table.getDbName());
  }

  private List<Order> extractSortOrders(final List<org.apache.hadoop.hive.metastore.api.Order> hiveOrders) {
    final List<Order> sortOrders = new ArrayList<>();
    if (hiveOrders == null) {
      return sortOrders;
    }

    for (final org.apache.hadoop.hive.metastore.api.Order hiveOrder : hiveOrders) {
      final Order order = new Order().withSortOrder(hiveOrder.getOrder()).withColumn(hiveOrder.getCol());
      sortOrders.add(order);
    }
    return sortOrders;
  }

  private List<Column> extractColumns(final List<FieldSchema> colList) {
    final List<Column> columns = new ArrayList<>();
    if (colList == null) {
      return columns;
    }

    for (final FieldSchema fieldSchema : colList) {
      final Column col = new Column()
          .withName(stringCleaner.clean(fieldSchema.getName()))
          .withType(stringCleaner.clean(fieldSchema.getType()))
          .withComment(stringCleaner.shortTo255Chars(stringCleaner.clean(fieldSchema.getComment())));

      columns.add(col);
    }
    return columns;
  }

  private Date convertMillisToTableDate(final Integer lastAccessTime) {
    if (lastAccessTime == 0) {
      return null;
    }
    try {
      return new Date(lastAccessTime);
    } catch (Exception e) {
      log.error("Error formatting table date", e);
    }
    return null;
  }
}
