/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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

import java.util.*;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;

public class HiveToGluePartitionComparator {
  /**
   * transient_lastDdlTime is set by Hive on DDL/write operations (create, alter, and
   * effectively on insert via auto-gathered stats), not on reads. It can be present in Hive
   * but absent from Glue's copy of a partition that was created or synced through a
   * different path, without any real schema/data drift — confirmed via a real Hive/Glue
   * partition comparison — so it's excluded from parameter equality checks.
   */
  private static final Set<String> VOLATILE_PARAMETER_KEYS = Collections.singleton("transient_lastDdlTime");

  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();
  private final S3PrefixNormalizer s3PrefixNormalizer = new S3PrefixNormalizer();

  /**
   * Returns true if the Glue Partition matches the Hive Partition, false
   * otherwise.
   */
  public boolean equals(org.apache.hadoop.hive.metastore.api.Partition hivePartition,
      Partition gluePartition) {
    if (hivePartition == null || gluePartition == null)
      return false;

    // Compare values (partition keys), cleaning Hive values to match Glue
    List<String> hiveValues = hivePartition.getValues();
    List<String> glueValues = gluePartition.getValues();
    if (hiveValues == null && glueValues == null)
      // not sure if this is really needed.
      return true;
    if (hiveValues == null || glueValues == null)
      return false;
    if (hiveValues.size() != glueValues.size())
      return false;
    for (int i = 0; i < hiveValues.size(); i++) {
      String hv = cleaner.removeNonUnicodeChars(hiveValues.get(i));
      String gv = glueValues.get(i);
      if (!Objects.equals(hv, gv))
        return false;
    }

    // Compare parameters (null-safe, ignore order, ignore volatile keys)
    if (filteredParametersNotEqual(hivePartition.getParameters(), gluePartition.getParameters()))
      return false;

    // lastAccessTime is not schema/data-relevant and changes on ordinary Hive reads, so it
    // is intentionally excluded from this comparison.

    // Compare StorageDescriptor
    org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd = hivePartition.getSd();
    StorageDescriptor glueSd = gluePartition.getStorageDescriptor();
    if (!storageDescriptorEquals(hiveSd, glueSd))
      return false;

    return true;
  }

  private boolean storageDescriptorEquals(org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd,
      StorageDescriptor glueSd) {
    if (hiveSd == null && glueSd == null)
      return true;
    if (hiveSd == null || glueSd == null)
      return false;

    // Bucket columns
    if (!Objects.equals(hiveSd.getBucketCols(), glueSd.getBucketColumns()))
      return false;

    // Columns
    if (!columnsEqualsCleaned(hiveSd.getCols(), glueSd.getColumns()))
      return false;

    // Compressed
    if (hiveSd.isCompressed() != glueSd.getCompressed())
      return false;

    // InputFormat
    if (!Objects.equals(hiveSd.getInputFormat(), glueSd.getInputFormat()))
      return false;

    String normalizedHiveLocation = s3PrefixNormalizer.normalizeLocation(hiveSd.getLocation());
    if (!Objects.equals(normalizedHiveLocation, glueSd.getLocation()))
      return false;

    // NumBuckets
    if (hiveSd.getNumBuckets() != glueSd.getNumberOfBuckets())
      return false;

    // OutputFormat
    if (!Objects.equals(hiveSd.getOutputFormat(), glueSd.getOutputFormat()))
      return false;

    // Parameters
    if (filteredParametersNotEqual(hiveSd.getParameters(), glueSd.getParameters()))
      return false;

    // SerDeInfo
    if (!serdeEquals(hiveSd.getSerdeInfo(), glueSd.getSerdeInfo()))
      return false;

    // Sort columns
    if (!sortOrdersEquals(hiveSd.getSortCols(), glueSd.getSortColumns()))
      return false;

    // StoredAsSubDirectories (primitive boolean)
    if (hiveSd.isStoredAsSubDirectories() != glueSd.getStoredAsSubDirectories())
      return false;

    return true;
  }

  // Compare columns, cleaning Hive comments before comparing to cleaned Glue
  // comments
  private boolean columnsEqualsCleaned(List<org.apache.hadoop.hive.metastore.api.FieldSchema> hiveCols,
      List<Column> glueCols) {
    if (hiveCols == null && glueCols == null)
      return true;
    if (hiveCols == null || glueCols == null)
      return false;
    if (hiveCols.size() != glueCols.size())
      return false;
    for (int i = 0; i < hiveCols.size(); i++) {
      org.apache.hadoop.hive.metastore.api.FieldSchema h = hiveCols.get(i);
      Column g = glueCols.get(i);
      if (!Objects.equals(h.getName(), g.getName()))
        return false;
      if (!Objects.equals(h.getType(), g.getType()))
        return false;
      // Clean Hive comment before comparing to cleaned Glue comment
      String cleanedHiveComment = cleaner.removeNonUnicodeChars(h.getComment());
      cleanedHiveComment = cleaner.truncateToMaxAllowedChars(cleanedHiveComment, 254);
      if (!Objects.equals(cleanedHiveComment, g.getComment()))
        return false;
    }
    return true;
  }

  private boolean serdeEquals(org.apache.hadoop.hive.metastore.api.SerDeInfo hiveSerde, SerDeInfo glueSerde) {
    if (hiveSerde == null && glueSerde == null)
      return true;
    if (hiveSerde == null || glueSerde == null)
      return false;
    if (!Objects.equals(hiveSerde.getName(), glueSerde.getName()))
      return false;
    if (filteredParametersNotEqual(hiveSerde.getParameters(), glueSerde.getParameters()))
      return false;
    if (!Objects.equals(hiveSerde.getSerializationLib(), glueSerde.getSerializationLibrary()))
      return false;
    return true;
  }

  /**
   * Compare parameter maps ignoring known-volatile Hive keys that change on ordinary reads
   * or DDL touches without reflecting a real schema/data change.
   */
  private boolean filteredParametersNotEqual(Map<String, String> hiveParams, Map<String, String> glueParams) {
    Map<String, String> filteredHive = new HashMap<>(hiveParams == null ? Collections.emptyMap() : hiveParams);
    Map<String, String> filteredGlue = new HashMap<>(glueParams == null ? Collections.emptyMap() : glueParams);
    filteredHive.keySet().removeAll(VOLATILE_PARAMETER_KEYS);
    filteredGlue.keySet().removeAll(VOLATILE_PARAMETER_KEYS);
    return !Objects.equals(filteredHive, filteredGlue);
  }

  private boolean sortOrdersEquals(List<org.apache.hadoop.hive.metastore.api.Order> hiveOrders,
      List<Order> glueOrders) {
    if (hiveOrders == null && glueOrders == null)
      return true;
    if (hiveOrders == null || glueOrders == null)
      return false;
    if (hiveOrders.size() != glueOrders.size())
      return false;
    for (int i = 0; i < hiveOrders.size(); i++) {
      org.apache.hadoop.hive.metastore.api.Order h = hiveOrders.get(i);
      Order g = glueOrders.get(i);
      if (!Objects.equals(h.getCol(), g.getColumn()))
        return false;
      if (h.getOrder() != g.getSortOrder())
        return false;
    }
    return true;
  }

}
