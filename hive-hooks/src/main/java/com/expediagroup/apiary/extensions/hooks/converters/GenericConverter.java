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

import lombok.extern.slf4j.Slf4j;

import com.expediagroup.apiary.extensions.hooks.config.Configuration;

@Slf4j
public abstract class GenericConverter {

  private final Configuration configuration;

  protected GenericConverter(Configuration configuration) {
    this.configuration = configuration;
  }

  /**
   * Converts a path for the given Table.
   *
   * @param table Table location to potentially alter.
   * @return true if table location is altered, false otherwise.
   */
  public boolean convertPath(Table table) {
    if (!configuration.isPathConversionEnabled()) {
      log.trace("[{}-Filter] pathConversion is disabled. Skipping path conversion for table.",
          getClass().getSimpleName());
      return false;
    }

    StorageDescriptor sd = table.getSd();
    log.debug("[{}-Filter] Examining table location: {}", getClass().getSimpleName(), sd.getLocation());
    return convertPath(sd);
  }

  /**
   * Converts a path for the given Partition.
   *
   * @param partition Partition location to potentially alter.
   * @return true if partition location is altered, false otherwise.
   */
  public boolean convertPath(Partition partition) {
    if (!configuration.isPathConversionEnabled()) {
      log.trace("[{}-Filter] pathConversion is disabled. Skipping path conversion for partition.",
          getClass().getSimpleName());
      return false;
    }

    StorageDescriptor sd = partition.getSd();
    log.debug("[{}-Filter] Examining partition location: {}", getClass().getSimpleName(), sd.getLocation());
    return convertPath(sd);
  }

  protected Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Converts a path for the given StorageDescriptor.
   *
   * @param sd StorageDescriptor to potentially alter.
   * @return true if location is altered, false otherwise.
   */
  public abstract boolean convertPath(StorageDescriptor sd);
}
