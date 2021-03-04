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
package com.expediagroup.apiary.extensions.hooks.pathconversion.converters;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.extern.slf4j.Slf4j;

import com.expediagroup.apiary.extensions.hooks.pathconversion.config.Configuration;

@Slf4j
public abstract class GenericConverter {

  private final Configuration configuration;

  GenericConverter(Configuration configuration) {
    this.configuration = configuration;
  }

  public boolean convertPath(Table table) {
    if (!configuration.isPathConversionEnabled()) {
      return false;
    }

    StorageDescriptor sd = table.getSd();
    log.debug("[{}-Filter] Examining table location: {}", getClass().getSimpleName(), sd.getLocation());
    return convertPath(sd);
  }

  public boolean convertPath(Partition partition) {
    if (!configuration.isPathConversionEnabled()) {
      return false;
    }

    StorageDescriptor sd = partition.getSd();
    log.debug("[{}-Filter] Examining partition location: {}", getClass().getSimpleName(), sd.getLocation());
    return convertPath(sd);
  }

  Configuration getConfiguration() {
    return configuration;
  }

  public abstract boolean convertPath(StorageDescriptor sd);
}
