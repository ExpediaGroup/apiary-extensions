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
package com.expediagroup.apiary.extensions.hooks.pathconversion.converters;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import lombok.extern.slf4j.Slf4j;

import com.google.common.base.Strings;

import com.expediagroup.apiary.extensions.hooks.config.Configuration;
import com.expediagroup.apiary.extensions.hooks.converters.GenericConverter;
import com.expediagroup.apiary.extensions.hooks.pathconversion.config.PathConversionConfiguration;
import com.expediagroup.apiary.extensions.hooks.pathconversion.models.PathConversion;

@Slf4j
public class PathConverter extends GenericConverter {

  static final String SD_INFO_PATH_PARAMETER = "path";
  static final String TABLE_AVRO_SCHEMA_URL_PARAMETER = "avro.schema.url";

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
      log.trace("PathConversion is disabled. Skipping path conversion for table.");
      return false;
    }
    boolean tableConverted = false;
    if (table.isSetParameters()) {
      String parameterPath = table.getParameters().get(TABLE_AVRO_SCHEMA_URL_PARAMETER);
      if (!Strings.isNullOrEmpty(parameterPath)) {
        String newParameterPath = convert(parameterPath);
        Map<String, String> parameters = new HashMap<>(table.getParameters());
        parameters.put(TABLE_AVRO_SCHEMA_URL_PARAMETER, newParameterPath);
        table.setParameters(parameters);
        tableConverted |= !parameterPath.equals(newParameterPath);
      }
    }

    StorageDescriptor sd = table.getSd();
    log.debug("Examining table location: {}", sd.getLocation());
    tableConverted |= convertStorageDescriptor(sd, Warehouse.getQualifiedName(table));
    return tableConverted;
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
    return convertStorageDescriptor(sd, Warehouse.getQualifiedName(partition));
  }

  @Override
  public boolean convertStorageDescriptor(StorageDescriptor sd) {
    return convertStorageDescriptor(sd, "");
  }

  private boolean convertStorageDescriptor(StorageDescriptor sd, String qualifiedTableName) {
    boolean pathConverted = false;
    String currentLocation = sd.getLocation();
    if (!Strings.isNullOrEmpty(currentLocation)) {
      sd.setLocation(convert(currentLocation));
      log.info("Switching storage location {} to {}.", currentLocation, sd.getLocation());
      pathConverted = !currentLocation.equals(sd.getLocation());
    } else {
      log.info("Switching storage location not possible empty/null location, table: {}", qualifiedTableName);
    }
    if (sd.isSetSerdeInfo() && sd.getSerdeInfo().isSetParameters()) {
      String parameterPath = sd.getSerdeInfo().getParameters().get(SD_INFO_PATH_PARAMETER);
      if (!Strings.isNullOrEmpty(parameterPath)) {
        String newParameterPath = convert(parameterPath);
        Map<String, String> parameters = new HashMap<>(sd.getSerdeInfo().getParameters());
        parameters.put(SD_INFO_PATH_PARAMETER, newParameterPath);
        sd.getSerdeInfo().setParameters(parameters);
        pathConverted |= !parameterPath.equals(newParameterPath);
      }
    }
    return pathConverted;
  }

  private String convert(String location) {
    String newLocation = location;
    boolean pathConverted = false;
    for (PathConversion pathConversion : getConfiguration().getPathConversions()) {
      Matcher matcher = pathConversion.pathPattern.matcher(newLocation);
      if (matcher.find()) {
        StringBuilder newLocationBuilder = new StringBuilder(newLocation);
        int offset = 0;

        for (Integer captureGroup : pathConversion.captureGroups) {
          if (hasCaptureGroup(matcher, captureGroup, newLocation)) {
            newLocationBuilder
                .replace(matcher.start(captureGroup) + offset, matcher.end(captureGroup) + offset,
                    pathConversion.replacementValue);
            offset += pathConversion.replacementValue.length() - matcher.group(captureGroup).length();
            pathConverted = true;
          }
        }

        if (pathConverted) {
          newLocation = newLocationBuilder.toString();
        }
      }
    }
    return newLocation;
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
