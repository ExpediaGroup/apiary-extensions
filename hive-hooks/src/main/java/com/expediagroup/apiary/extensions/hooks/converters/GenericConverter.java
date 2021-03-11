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
package com.expediagroup.apiary.extensions.hooks.converters;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.expediagroup.apiary.extensions.hooks.config.Configuration;

@Slf4j
public abstract class GenericConverter {

  @Getter private final Configuration configuration;

  protected GenericConverter(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Apply a conversion for a given table.
   *
   * @param table Table to potentially alter.
   * @return true if table is altered, false otherwise.
   */
  public abstract boolean convertTable(Table table);

  /**
   * Apply a conversion for the given Partition.
   *
   * @param partition Partition to potentially alter.
   * @return true if partition is altered, false otherwise.
   */
  public abstract boolean convertPartition(Partition partition);

  /**
   * Apply a conversion for the given StorageDescriptor.
   *
   * @param sd StorageDescriptor to potentially alter.
   * @return true if SD is altered, false otherwise.
   */
  public abstract boolean convertStorageDescriptor(StorageDescriptor sd);
}
