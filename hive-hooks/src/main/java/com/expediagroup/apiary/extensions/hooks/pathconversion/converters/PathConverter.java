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

import java.util.regex.Matcher;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.extern.slf4j.Slf4j;

import com.expediagroup.apiary.extensions.hooks.config.Configuration;
import com.expediagroup.apiary.extensions.hooks.converters.GenericConverter;
import com.expediagroup.apiary.extensions.hooks.pathconversion.config.PathConversionConfiguration;
import com.expediagroup.apiary.extensions.hooks.pathconversion.models.PathConversion;

@Slf4j
public class PathConverter extends GenericConverter {

  public PathConverter(Configuration configuration) {
    super(configuration);
  }

  @Override
  public PathConversionConfiguration getConfiguration() {
    return (PathConversionConfiguration) super.getConfiguration();
  }

  /**
   * Converts a path for the given Table.
   *
   * @param table Table location to potentially alter.
   * @return true if table location is altered, false otherwise.
   */
  @Override
  public boolean convertTable(Table table) {
    if (!getConfiguration().isPathConversionEnabled()) {
      log.trace("[{}-Filter] pathConversion is disabled. Skipping path conversion for table.",
          getClass().getSimpleName());
      return false;
    }

    StorageDescriptor sd = table.getSd();
    log.debug("[{}-Filter] Examining table location: {}", getClass().getSimpleName(), sd.getLocation());
    return convertStorageDescriptor(sd);
  }

  /**
   * Converts a path for the given Partition.
   *
   * @param partition Partition location to potentially alter.
   * @return true if partition location is altered, false otherwise.
   */
  @Override
  public boolean convertPartition(Partition partition) {
    if (!getConfiguration().isPathConversionEnabled()) {
      log.trace("PathConversion is disabled. Skipping path conversion for partition.");
      return false;
    }

    StorageDescriptor sd = partition.getSd();
    log.debug("Examining partition location: {}", sd.getLocation());
    return convertStorageDescriptor(sd);
  }

  @Override
  public boolean convertStorageDescriptor(StorageDescriptor sd) {
    boolean pathConverted = false;
    for (PathConversion pathConversion : getConfiguration().getPathConversions()) {
      Matcher matcher = pathConversion.pathPattern.matcher(sd.getLocation());
      if (matcher.find()) {
        for (Integer captureGroup : pathConversion.captureGroups) {
          if (hasCaptureGroup(matcher, captureGroup, sd.getLocation())) {
            String newLocation = sd.getLocation().replace(matcher.group(captureGroup), pathConversion.replacementValue);
            log.info("Switching storage location {} to {}.", sd.getLocation(), newLocation);
            sd.setLocation(newLocation);
            pathConverted = true;
          }
        }
      }
    }

    return pathConverted;
  }

  private boolean hasCaptureGroup(Matcher matcher, int groupNumber, String location) {
    try {
      matcher.group(groupNumber);
      return true;
    } catch (IndexOutOfBoundsException ex) {
      log.warn("No capture group number {} found on location[{}]", groupNumber, location);
      return false;
    }
  }
}
