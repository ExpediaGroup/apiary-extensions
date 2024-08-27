/**
 * Copyright (C) 2018-2022 Expedia, Inc.
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

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.types.Types;

import com.google.common.collect.Maps;

public class IcebergTableOperations {
  protected static Table simpleIcebergTable(String database, String tableName, Schema schema, PartitionSpec partitionSpec,
      SortOrder sortOrder) {
    final long currentTimeMillis = System.currentTimeMillis();

    Table newTable =
        new Table(
            tableName,
            database,
            System.getProperty("user.name"),
            (int) currentTimeMillis / 1000,
            (int) currentTimeMillis / 1000,
            Integer.MAX_VALUE,
            null,
            Collections.emptyList(),
            Maps.newHashMap(),
            null,
            null,
            TableType.EXTERNAL_TABLE.toString());

    newTable.getParameters().put("EXTERNAL", "TRUE");
    newTable.setSd(storageDescriptor(schema));
    setHmsTableParameters(newTable, schema, partitionSpec, sortOrder);
    return newTable;
  }

  protected static Schema simpleIcebergSchema() {
    return new Schema(required(1, "col1", Types.StringType.get()),
        required(2, "col2", Types.StringType.get()), required(3, "col3", Types.IntegerType.get()));
  }

  protected static PartitionSpec simpleIcebergPartitionSpec() {
    return builderFor(simpleIcebergSchema()).identity("col1").build();
  }

  private static StorageDescriptor storageDescriptor(Schema schema) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(HiveSchemaUtil.convert(schema));
    storageDescriptor.setLocation("location");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(Maps.newHashMap());
    storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
    storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private static void setHmsTableParameters(Table tbl, Schema schema, PartitionSpec partitionSpec, SortOrder sortOrder) {
    Map<String, String> parameters =
        Optional.ofNullable(tbl.getParameters()).orElseGet(Maps::newHashMap);

    parameters.put(TableProperties.UUID, UUID.randomUUID().toString());
    parameters.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ENGLISH));
    parameters.put(
        hive_metastoreConstants.META_TABLE_STORAGE,
        "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");

    setSchema(schema, parameters);
    setPartitionSpec(partitionSpec, parameters);
    setSortOrder(sortOrder, parameters);

    tbl.setParameters(parameters);
  }

  private static void setSchema(Schema schema, Map<String, String> parameters) {
    parameters.remove(TableProperties.CURRENT_SCHEMA);
    if (schema != null) {
      String schemaJSON = SchemaParser.toJson(schema);
      parameters.put(TableProperties.CURRENT_SCHEMA, schemaJSON);
    }
  }

  private static void setPartitionSpec(PartitionSpec partitionSpec, Map<String, String> parameters) {
    parameters.remove(TableProperties.DEFAULT_PARTITION_SPEC);
    if (partitionSpec != null && partitionSpec.isPartitioned()) {
      String spec = PartitionSpecParser.toJson(partitionSpec);
      parameters.put(TableProperties.DEFAULT_PARTITION_SPEC, spec);
    }
  }

  private static void setSortOrder(SortOrder sortOrder, Map<String, String> parameters) {
    parameters.remove(TableProperties.DEFAULT_SORT_ORDER);
    if (sortOrder != null && sortOrder.isSorted()) {
      String sortOrderJSON = SortOrderParser.toJson(sortOrder);
      parameters.put(TableProperties.DEFAULT_SORT_ORDER, sortOrderJSON);
    }
  }
}
