/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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

import static org.junit.Assert.*;

import java.util.*;
import java.util.Date;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Before;
import org.junit.Test;

// Using fully-qualified names for Glue model types in helper methods to avoid
// collisions with Hive types.

public class HiveToGluePartitionComparatorTest {
  private final HiveToGluePartitionComparator comparator = new HiveToGluePartitionComparator();
  private final GlueMetadataStringCleaner cleaner = new GlueMetadataStringCleaner();

  @Before
  public void setUp() {
    // No transformer used in these tests; we build Glue Partition objects directly.
  }

  @Test
  public void testEqualsUsingTransformer() {
    Partition hivePartition = createCompleteHivePartition();
    com.amazonaws.services.glue.model.Partition gluePartition = buildGluePartitionFromHive(hivePartition);
    assertTrue("Transformed partition should equal original",
        comparator.equals(hivePartition, gluePartition));
  }

  @Test
  public void testEqualsWithCleanedValues() {
    // Create Hive partition with problematic values
    Partition hivePartition = createCompleteHivePartition();
    hivePartition.setValues(Arrays.asList("val1\u0000", "val2\u0001"));
    hivePartition.getSd().getCols().get(0).setComment("comment1\u0000");
    hivePartition.getSd().getCols().get(1).setComment(repeat('a', 300));

    // Build Glue partition from Hive partition and apply cleaning similar to
    // transformer
    com.amazonaws.services.glue.model.Partition gluePartition = buildGluePartitionFromHive(hivePartition);

    assertTrue("Cleaned values should match, comparator also cleans", comparator.equals(hivePartition, gluePartition));
  }

  @Test
  public void testLocationNormalization() {
    Partition hivePartition = createCompleteHivePartition();
    hivePartition.getSd().setLocation("s3a://my-bucket/path/to/table");

    com.amazonaws.services.glue.model.Partition gluePartition = buildGluePartitionFromHive(hivePartition);
    assertTrue("Location normalization should match", comparator.equals(hivePartition, gluePartition));
  }

  @Test
  public void testLastAccessTimeMismatch() {
    Partition hivePartition = createCompleteHivePartition();
    hivePartition.setLastAccessTime(123456789);

    com.amazonaws.services.glue.model.Partition gluePartition = buildGluePartitionFromHive(hivePartition);
    // Force a different timestamp to test inequality
    gluePartition.setLastAccessTime(new Date(987654321L));

    assertFalse("Different timestamps should not match",
        comparator.equals(hivePartition, gluePartition));
  }

  @Test
  public void testParameterMismatch() {
    Partition hivePartition = createCompleteHivePartition();
    com.amazonaws.services.glue.model.Partition gluePartition = buildGluePartitionFromHive(hivePartition);
    // Add an extra parameter to force inequality
    Map<String, String> modifiedParams = new HashMap<>(gluePartition.getParameters());
    modifiedParams.put("extra_key", "extra_value");
    gluePartition.setParameters(modifiedParams);

    assertFalse("Different parameters should not match",
        comparator.equals(hivePartition, gluePartition));
  }

  // Helper to build a com.amazonaws.services.glue.model.Partition from a Hive
  // Partition
  private com.amazonaws.services.glue.model.Partition buildGluePartitionFromHive(
      org.apache.hadoop.hive.metastore.api.Partition hivePartition) {
    com.amazonaws.services.glue.model.Partition glue = new com.amazonaws.services.glue.model.Partition();
    // Values: clean Hive values similar to GlueMetadataStringCleaner
    if (hivePartition.getValues() != null) {
      List<String> cleanedValues = new ArrayList<>();
      for (String v : hivePartition.getValues()) {
        cleanedValues.add(cleaner.removeNonUnicodeChars(v));
      }
      glue.setValues(cleanedValues);
    }

    // Parameters
    glue.setParameters(hivePartition.getParameters());

    // LastAccessTime
    Integer hiveLastAccess = hivePartition.getLastAccessTime();
    Date hiveDate = (hiveLastAccess == 0) ? null : new Date(hiveLastAccess);
    glue.setLastAccessTime(hiveDate);

    // StorageDescriptor
    org.apache.hadoop.hive.metastore.api.StorageDescriptor hsd = hivePartition.getSd();
    if (hsd != null) {
      com.amazonaws.services.glue.model.StorageDescriptor gsd = new com.amazonaws.services.glue.model.StorageDescriptor();
      // Columns
      if (hsd.getCols() != null) {
        List<com.amazonaws.services.glue.model.Column> cols = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.FieldSchema fs : hsd.getCols()) {
          com.amazonaws.services.glue.model.Column c = new com.amazonaws.services.glue.model.Column();
          c.setName(fs.getName());
          c.setType(fs.getType());
          String cleanedComment = cleaner.removeNonUnicodeChars(fs.getComment());
          cleanedComment = cleaner.truncateToMaxAllowedChars(cleanedComment);
          c.setComment(cleanedComment);
          cols.add(c);
        }
        gsd.setColumns(cols);
      }
      // Bucket cols
      gsd.setBucketColumns(hsd.getBucketCols());
      // Compressed
      gsd.setCompressed(hsd.isCompressed());
      // Input/Output format
      gsd.setInputFormat(hsd.getInputFormat());
      gsd.setLocation(new S3PrefixNormalizer().normalizeLocation(hsd.getLocation()));
      gsd.setNumberOfBuckets(hsd.getNumBuckets());
      gsd.setOutputFormat(hsd.getOutputFormat());
      gsd.setParameters(hsd.getParameters());
      // SerDeInfo
      if (hsd.getSerdeInfo() != null) {
        com.amazonaws.services.glue.model.SerDeInfo si = new com.amazonaws.services.glue.model.SerDeInfo();
        si.setName(hsd.getSerdeInfo().getName());
        si.setSerializationLibrary(hsd.getSerdeInfo().getSerializationLib());
        si.setParameters(hsd.getSerdeInfo().getParameters());
        gsd.setSerdeInfo(si);
      }
      // Sort cols - map Orders if present
      if (hsd.getSortCols() != null) {
        List<com.amazonaws.services.glue.model.Order> orders = new ArrayList<>();
        for (org.apache.hadoop.hive.metastore.api.Order o : hsd.getSortCols()) {
          com.amazonaws.services.glue.model.Order go = new com.amazonaws.services.glue.model.Order();
          go.setColumn(o.getCol());
          go.setSortOrder(o.getOrder());
          orders.add(go);
        }
        gsd.setSortColumns(orders);
      }
      gsd.setStoredAsSubDirectories(hsd.isStoredAsSubDirectories());
      glue.setStorageDescriptor(gsd);
    }

    // The GlueMetadataStringCleaner.cleanPartition expects a PartitionInput; we
    // already cleaned values and comments above, so return the built Glue
    // Partition directly.
    return glue;
  }

  private Partition createCompleteHivePartition() {
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("val1", "val2"));

    Map<String, String> params = new HashMap<>();
    params.put("k1", "v1");
    partition.setParameters(params);
    partition.setLastAccessTime(123456789);

    org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
    List<FieldSchema> cols = Arrays.asList(
        new FieldSchema("col1", "string", "comment1"),
        new FieldSchema("col2", "int", "comment2"));
    sd.setCols(cols);
    sd.setBucketCols(Arrays.asList("bucket1"));
    sd.setCompressed(true);
    sd.setInputFormat("inputFmt");
    sd.setLocation("s3://bucket/path");
    sd.setNumBuckets(2);
    sd.setOutputFormat("outputFmt");
    sd.setParameters(Collections.singletonMap("pkey", "pval"));
    sd.setSerdeInfo(new org.apache.hadoop.hive.metastore.api.SerDeInfo("serde", "lib",
        Collections.singletonMap("spk", "spv")));
    sd.setSortCols(new ArrayList<>());
    sd.setStoredAsSubDirectories(false);
    partition.setSd(sd);

    return partition;
  }

  private static String repeat(char c, int n) {
    char[] arr = new char[n];
    Arrays.fill(arr, c);
    return new String(arr);
  }
}
