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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;

@RunWith(MockitoJUnitRunner.class)
public class GluePartitionServiceTest {

  @Mock
  private AWSGlue mockGlueClient;

  private GluePartitionService service;
  private Table testTable;

  // No test subclass required — protected methods are accessible from tests in
  // same package

  @Before
  public void setUp() {
    service = new GluePartitionService(mockGlueClient, "test-prefix-");
    testTable = createTestTable();
  }

  @Test
  public void testBatchCreatePartitions_Success() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(50);

    service.batchCreatePartitions(testTable, hivePartitions);

    ArgumentCaptor<BatchCreatePartitionRequest> captor = ArgumentCaptor.forClass(BatchCreatePartitionRequest.class);
    verify(mockGlueClient, times(1)).batchCreatePartition(captor.capture());

    BatchCreatePartitionRequest request = captor.getValue();
    assertThat(request.getPartitionInputList().size(), is(50));
    assertThat(request.getDatabaseName(), is("test-prefix-test_db"));
    assertThat(request.getTableName(), is("test_table"));
  }

  @Test
  public void testBatchCreatePartitions_PayloadTooLarge_ReducesBatchSize() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(100);

    // First call fails with 413, second succeeds
    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    when(mockGlueClient.batchCreatePartition(argThat(req -> req.getPartitionInputList().size() == 100)))
        .thenThrow(exception413);

    service.batchCreatePartitions(testTable, hivePartitions);

    // Should have been called 3 times: 1 failure at 100, then 2 successes at 50
    // each
    verify(mockGlueClient, times(3)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchCreatePartitions_PayloadTooLarge_MultipleReductions() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(100);

    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    // Fail at 100, 50, and 25, succeed at 12
    when(mockGlueClient.batchCreatePartition(argThat(req -> {
      int size = req.getPartitionInputList().size();
      return size == 100 || size == 50 || size == 25;
    }))).thenThrow(exception413);

    service.batchCreatePartitions(testTable, hivePartitions);

    // Multiple calls with decreasing batch sizes
    verify(mockGlueClient, times(12)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchCreateOperations_NonRetryableException_Rethrows() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(10);

    AmazonServiceException exception500 = new AmazonServiceException("Internal server error");
    exception500.setStatusCode(500);

    when(mockGlueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class))).thenThrow(exception500);

    try {
      service.batchCreatePartitions(testTable, hivePartitions);
      fail("Expected AmazonServiceException to be thrown");
    } catch (AmazonServiceException e) {
      assertThat(e.getStatusCode(), is(500));
    }

    // Should only be called once since it's not a 413 error
    verify(mockGlueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchCreateOperations_BatchSizeOne_StillRetries() {
    // When batch size reduces to 1 and still fails with 413, it should throw
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(1);

    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    when(mockGlueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class))).thenThrow(exception413);

    try {
      service.batchCreatePartitions(testTable, hivePartitions);
      fail("Expected AmazonServiceException to be thrown");
    } catch (AmazonServiceException e) {
      assertThat(e.getStatusCode(), is(413));
    }

    // Should retry multiple times reducing batch size until it reaches 1 and fails
    // Initial batch of 1 at size 100, then retries at 50, 25, 12, 6, 3, 1
    verify(mockGlueClient, times(7)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchCreateOperations_ProcessesAllPartitionsEvenWithRetries() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = createHivePartitions(150);

    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    // Fail only the first batch
    when(mockGlueClient.batchCreatePartition(argThat(req -> req.getPartitionInputList().size() == 100)))
        .thenThrow(exception413);

    service.batchCreatePartitions(testTable, hivePartitions);

    ArgumentCaptor<BatchCreatePartitionRequest> captor = ArgumentCaptor.forClass(BatchCreatePartitionRequest.class);
    verify(mockGlueClient, times(4)).batchCreatePartition(captor.capture());

    List<BatchCreatePartitionRequest> requests = captor.getAllValues();

    // Calculate total partitions processed
    int totalProcessed = requests.stream()
        .filter(req -> req.getPartitionInputList().size() <= 50) // Only count successful calls
        .mapToInt(req -> req.getPartitionInputList().size())
        .sum();

    // Should process all 150 partitions (2 batches of 50 + 1 batch of 50)
    assertThat(totalProcessed, is(150));
  }

  @Test
  public void testBatchCreateOperations_EmptyList_DoesNothing() {
    List<org.apache.hadoop.hive.metastore.api.Partition> hivePartitions = Collections.emptyList();

    service.batchCreatePartitions(testTable, hivePartitions);

    verify(mockGlueClient, times(0)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchCreateOperations_NullList_DoesNothing() {
    service.batchCreatePartitions(testTable, null);

    verify(mockGlueClient, times(0)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  public void testBatchUpdatePartitions_Success() {
    HashMap<org.apache.hadoop.hive.metastore.api.Partition, com.amazonaws.services.glue.model.Partition> partitionsToUpdate = new HashMap<>();
    for (int i = 0; i < 12; i++) {
      partitionsToUpdate.put(createHivePartition(i), createGluePartition(i));
    }

    service.batchUpdatePartitions(testTable, partitionsToUpdate);

    ArgumentCaptor<com.amazonaws.services.glue.model.BatchUpdatePartitionRequest> captor = ArgumentCaptor
        .forClass(com.amazonaws.services.glue.model.BatchUpdatePartitionRequest.class);
    verify(mockGlueClient, times(1)).batchUpdatePartition(captor.capture());

    com.amazonaws.services.glue.model.BatchUpdatePartitionRequest request = captor.getValue();
    assertThat(request.getEntries().size(), is(12));
    assertThat(request.getDatabaseName(), is("test-prefix-test_db"));
    assertThat(request.getTableName(), is("test_table"));
  }

  @Test
  public void testBatchUpdatePartitions_PayloadTooLarge_ReducesBatchSize() {
    HashMap<org.apache.hadoop.hive.metastore.api.Partition, com.amazonaws.services.glue.model.Partition> partitionsToUpdate = new HashMap<>();
    // Create enough unique partitions (year + month + day combinations)
    for (int year = 2020; year < 2025; year++) {
      for (int month = 1; month <= 12; month++) {
        for (int day = 1; day <= 3; day++) {
          int index = ((year - 2020) * 12 * 3) + ((month - 1) * 3) + (day - 1);
          if (index >= 150)
            break;
          partitionsToUpdate.put(createHivePartitionForYMD(year, month, day),
              createGluePartitionForYMD(year, month, day));
        }
      }
    }

    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    // First batch of 100 fails, then succeed with 50
    when(mockGlueClient.batchUpdatePartition(argThat(req -> req.getEntries().size() == 100)))
        .thenThrow(exception413);

    service.batchUpdatePartitions(testTable, partitionsToUpdate);

    // Should be called 4 times: 1 failure at 100, then 3 successes at 50 each
    verify(mockGlueClient, times(4))
        .batchUpdatePartition(any(com.amazonaws.services.glue.model.BatchUpdatePartitionRequest.class));
  }

  @Test
  public void testBatchDeletePartitions_PayloadTooLarge_ReducesBatchSize() {
    List<com.amazonaws.services.glue.model.Partition> gluePartitions = createGluePartitions(25);

    AmazonServiceException exception413 = new AmazonServiceException("Request entity too large");
    exception413.setStatusCode(413);

    // First attempt with 25 fails, then retry with smaller batches
    when(mockGlueClient.batchDeletePartition(argThat(req -> req.getPartitionsToDelete().size() == 25)))
        .thenThrow(exception413);

    service.batchDeletePartitions(testTable, gluePartitions);

    // Should be called 4 times: 1 failure at 25, then 3 successes at 12, 12, 1
    verify(mockGlueClient, times(4))
        .batchDeletePartition(any(com.amazonaws.services.glue.model.BatchDeletePartitionRequest.class));
  }

  // Helper methods

  private Table createTestTable() {
    Table table = new Table();
    table.setDbName("test_db");
    table.setTableName("test_table");

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Arrays.asList(
        new FieldSchema("col1", "string", "comment1"),
        new FieldSchema("col2", "int", "comment2")));
    sd.setLocation("s3://test-bucket/test-path");
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    sd.setSerdeInfo(serDeInfo);
    table.setSd(sd);

    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("year", "string", "year partition"),
        new FieldSchema("month", "string", "month partition")));

    return table;
  }

  private List<org.apache.hadoop.hive.metastore.api.Partition> createHivePartitions(int count) {
    List<org.apache.hadoop.hive.metastore.api.Partition> partitions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      partitions.add(createHivePartition(i));
    }
    return partitions;
  }

  private org.apache.hadoop.hive.metastore.api.Partition createHivePartition(int index) {
    List<String> values = Arrays.asList("2024", String.format("%02d", (index % 12) + 1));
    String location = "s3://test-bucket/test-path/year=2024/month=" + String.format("%02d", (index % 12) + 1);
    return buildHivePartition(values, location);
  }

  private List<com.amazonaws.services.glue.model.Partition> createGluePartitions(int count) {
    List<com.amazonaws.services.glue.model.Partition> partitions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      partitions.add(createGluePartition(i));
    }
    return partitions;
  }

  private com.amazonaws.services.glue.model.Partition createGluePartition(int index) {
    List<String> values = Arrays.asList("2024", String.format("%02d", (index % 12) + 1));
    String location = "s3://test-bucket/test-path/year=2024/month=" + String.format("%02d", (index % 12) + 1);
    return buildGluePartition(values, location);
  }

  // Helper to reduce duplication for creating hive Partition used in batch update
  // tests
  private org.apache.hadoop.hive.metastore.api.Partition createHivePartitionForYMD(int year, int month, int day) {
    List<String> values = Arrays.asList(String.valueOf(year), String.format("%02d", month), String.format("%02d", day));
    String location = "s3://test-bucket/test-path/year=" + year + "/month=" + String.format("%02d", month)
        + "/day=" + String.format("%02d", day);
    return buildHivePartition(values, location);
  }

  // Helper to reduce duplication for creating glue Partition used in batch update
  // tests
  private com.amazonaws.services.glue.model.Partition createGluePartitionForYMD(int year, int month, int day) {
    List<String> values = Arrays.asList(String.valueOf(year), String.format("%02d", month), String.format("%02d", day));
    String location = "s3://test-bucket/test-path/year=" + year + "/month=" + String.format("%02d", month)
        + "/day=" + String.format("%02d", day);
    return buildGluePartition(values, location);
  }

  // Generic builder for hive Partition to consolidate duplicated construction
  private org.apache.hadoop.hive.metastore.api.Partition buildHivePartition(List<String> values, String location) {
    org.apache.hadoop.hive.metastore.api.Partition hivePartition = new org.apache.hadoop.hive.metastore.api.Partition();
    hivePartition.setDbName("test_db");
    hivePartition.setTableName("test_table");
    hivePartition.setValues(values);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(Arrays.asList(
        new FieldSchema("col1", "string", "comment1"),
        new FieldSchema("col2", "int", "comment2")));
    sd.setLocation(location);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    sd.setSerdeInfo(serDeInfo);
    hivePartition.setSd(sd);

    return hivePartition;
  }

  // Generic builder for glue Partition to consolidate duplicated construction
  private com.amazonaws.services.glue.model.Partition buildGluePartition(List<String> values, String location) {
    com.amazonaws.services.glue.model.Partition partition = new com.amazonaws.services.glue.model.Partition();
    partition.setDatabaseName("test-prefix-test_db");
    partition.setTableName("test_table");
    partition.setValues(values);
    com.amazonaws.services.glue.model.StorageDescriptor sd = new com.amazonaws.services.glue.model.StorageDescriptor();
    sd.setLocation(location);
    partition.setStorageDescriptor(sd);
    return partition;
  }
}
