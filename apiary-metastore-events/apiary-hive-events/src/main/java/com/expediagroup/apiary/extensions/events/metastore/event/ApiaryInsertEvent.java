/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.event;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.InsertEvent;

public class ApiaryInsertEvent extends ApiaryListenerEvent {
  private static final long serialVersionUID = 1L;

  private String databaseName;
  private String tableName;
  private Map<String, String> partitionKeyValues;
  private List<String> files;
  private List<String> fileChecksums;

  ApiaryInsertEvent() {}

  public ApiaryInsertEvent(InsertEvent event) {
    super(event);

    Table table = event.getTableObj();
    databaseName = table.getDbName();
    tableName = table.getTableName();
    partitionKeyValues = createPartitionKeyValues(table, event.getPartitionObj());
    files = event.getFiles();
    fileChecksums = event.getFileChecksums();
  }

  public static Map<String, String> createPartitionKeyValues(Table table, Partition partition) {
    Map<String, String> partitionKeyValues = new LinkedHashMap<>();
    if (partition != null) {
      List<FieldSchema> partitionKeys = table.getPartitionKeys();
      List<String> partitionValues = partition.getValues();
      for (int i = 0; i < partitionKeys.size(); i++) {
        partitionKeyValues.put(partitionKeys.get(i).getName(), partitionValues.get(i));
      }
    }
    return partitionKeyValues;
  }

  @Override
  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public Map<String, String> getPartitionKeyValues() {
    return partitionKeyValues;
  }

  public List<String> getFiles() {
    return files;
  }

  public List<String> getFileChecksums() {
    return fileChecksums;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ApiaryInsertEvent)) {
      return false;
    }
    ApiaryInsertEvent other = (ApiaryInsertEvent) obj;
    return super.equals(other)
        && Objects.equals(databaseName, other.databaseName)
        && Objects.equals(tableName, other.tableName)
        && Objects.equals(partitionKeyValues, other.partitionKeyValues)
        && Objects.equals(files, other.files)
        && Objects.equals(fileChecksums, other.fileChecksums);
  }

}
