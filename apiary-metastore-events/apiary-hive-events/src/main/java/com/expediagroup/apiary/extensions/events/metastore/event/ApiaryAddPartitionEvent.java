/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;

public class ApiaryAddPartitionEvent extends ApiaryListenerEvent {
  private static final long serialVersionUID = 1L;

  private Table table;
  private List<Partition> partitions;

  ApiaryAddPartitionEvent() {}

  public ApiaryAddPartitionEvent(AddPartitionEvent event) {
    super(event);
    table = event.getTable();
    partitions = new ArrayList<>();
    Iterator<Partition> iterator = event.getPartitionIterator();
    while (iterator.hasNext()) {
      partitions.add(iterator.next());
    }
  }

  @Override
  public String getDatabaseName() {
    return table.getDbName();
  }

  @Override
  public String getTableName() {
    return table.getTableName();
  }

  public Table getTable() {
    return table;
  }

  public List<Partition> getPartitions() {
    return partitions;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ApiaryAddPartitionEvent)) {
      return false;
    }
    ApiaryAddPartitionEvent other = (ApiaryAddPartitionEvent) obj;
    return super.equals(other) && Objects.equals(table, other.table) && Objects.equals(partitions, other.partitions);
  }

}
