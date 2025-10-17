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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;

import com.amazonaws.services.glue.model.PartitionInput;

public class HiveToGlueTransformerTest {
  private final HiveToGlueTransformer transformer = new HiveToGlueTransformer(null);

  @Test
  public void testTransformPartitionCompleteFieldMapping() {
    // Test complete field mapping to ensure all fields are properly transformed
    Partition hivePartition = createCompleteHivePartition();

    PartitionInput result = transformer.transformPartition(hivePartition);

    // Verify all fields are mapped correctly
    assertNotNull("Result should not be null", result);
    assertEquals("Values should match", Arrays.asList("val1", "val2"), result.getValues());
    assertEquals("Parameters should match", createTestParameters(), result.getParameters());
    assertEquals("LastAccessTime should be converted", new Date(123456789), result.getLastAccessTime());

    // Verify StorageDescriptor is present and transformed
    assertNotNull("StorageDescriptor should not be null", result.getStorageDescriptor());
    assertEquals("Location should be normalized", "s3://test-bucket/path",
        result.getStorageDescriptor().getLocation());
  }

  /**
   * Helper method to create a minimal Hive Partition with required fields
   */
  private Partition createMinimalHivePartition() {
    Partition partition = new Partition();
    partition.setValues(Collections.emptyList());
    partition.setParameters(Collections.emptyMap());

    org.apache.hadoop.hive.metastore.api.StorageDescriptor sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
    sd.setCols(Collections.emptyList());
    sd.setBucketCols(Collections.emptyList());
    sd.setCompressed(false);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setLocation("s3://test-bucket/path");
    sd.setNumBuckets(0);
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setParameters(Collections.emptyMap());

    org.apache.hadoop.hive.metastore.api.SerDeInfo serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo();
    serdeInfo.setName("test-serde");
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    serdeInfo.setParameters(Collections.emptyMap());
    sd.setSerdeInfo(serdeInfo);

    sd.setSortCols(Collections.emptyList());
    sd.setStoredAsSubDirectories(false);
    partition.setSd(sd);

    return partition;
  }

  /**
   * Helper method to create a complete Hive Partition for comprehensive testing
   */
  private Partition createCompleteHivePartition() {
    Partition partition = createMinimalHivePartition();
    partition.setValues(Arrays.asList("val1", "val2"));
    partition.setParameters(createTestParameters());
    partition.setLastAccessTime(123456789);

    // Add some columns to the storage descriptor
    List<FieldSchema> columns = Arrays.asList(
        new FieldSchema("col1", "string", "First column"),
        new FieldSchema("col2", "int", "Second column"));
    partition.getSd().setCols(columns);
    partition.getSd().setLocation("s3a://test-bucket/path"); // Test normalization

    return partition;
  }

  /**
   * Helper method to create test parameters
   */
  private Map<String, String> createTestParameters() {
    Map<String, String> params = new HashMap<>();
    params.put("test.param1", "value1");
    params.put("test.param2", "value2");
    return params;
  }
}
