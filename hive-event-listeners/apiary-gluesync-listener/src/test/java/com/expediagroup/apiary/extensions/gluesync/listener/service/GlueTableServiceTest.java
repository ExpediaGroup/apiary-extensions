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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.ConcurrentModificationException;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.OperationTimeoutException;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateTableResult;

@RunWith(MockitoJUnitRunner.class)
public class GlueTableServiceTest {

  private static final String DB_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";
  private static final String LOCATION = "s3://bucket/test_table";
  private static final String LAST_DDL_TIME = "1751385266";

  @Mock
  private AWSGlue glueClient;
  @Mock
  private GluePartitionService gluePartitionService;

  private GlueTableService service;

  @Before
  public void setUp() {
    service = new GlueTableService(glueClient, gluePartitionService, null);
  }

  @Test
  public void deleteIfUnchanged_deletesWhenAllPropertiesMatch() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, null));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.DELETED));
    verify(glueClient).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenLastDdlTimeChanged() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResult(LOCATION, "1751385999", null));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenHiveTableReplacedByIceberg() {
    // Hive DROP event has no metadata_location; Glue now holds an Iceberg table
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, "s3://bucket/test_table/metadata/v1.metadata.json"));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenMetadataLocationDiverges() {
    when(glueClient.getTable(any(GetTableRequest.class)))
        .thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, "s3://bucket/test_table/metadata/v2.metadata.json"));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(
        hmsTable(LOCATION, LAST_DDL_TIME, "s3://bucket/test_table/metadata/v1.metadata.json"));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenHmsHasMetadataLocationButGlueDoesNot() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, null));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(
        hmsTable(LOCATION, LAST_DDL_TIME, "s3://bucket/test_table/metadata/v1.metadata.json"));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_notFoundWhenTableAbsentDuringGet() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenThrow(new EntityNotFoundException("not found"));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.NOT_FOUND));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_deletedConcurrentlyWhenTableDeletedBetweenGuardAndDelete() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, null));
    when(glueClient.deleteTable(any(DeleteTableRequest.class))).thenThrow(new EntityNotFoundException("not found"));

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.DELETED_CONCURRENTLY));
  }

  @Test
  public void deleteIfUnchanged_deletesWhenBothParamMapsAreNull() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResultNullParams());

    Table table = hmsTable(LOCATION, null, null);
    table.setParameters(null);
    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(table);

    assertThat(outcome, is(GlueTableService.DeleteOutcome.DELETED));
    verify(glueClient).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenHmsParamsNullButGlueHasParams() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResult(LOCATION, LAST_DDL_TIME, null));

    Table table = hmsTable(LOCATION, null, null);
    table.setParameters(null);
    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(table);

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void deleteIfUnchanged_skipsWhenGlueParamsNullButHmsHasParams() {
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResultNullParams());

    GlueTableService.DeleteOutcome outcome = service.deleteIfUnchanged(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.DeleteOutcome.SKIPPED));
    verify(glueClient, never()).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  public void update_doesNotSendVersionId_whenFeatureDisabled() {
    // isSendVersionId() defaults to false on the mock
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    GlueTableService.UpdateOutcome outcome = service.update(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.UpdateOutcome.UPDATED));
    verify(glueClient, never()).getTable(any(GetTableRequest.class));
    verify(glueClient).updateTable(captor.capture());
    assertThat(captor.getValue().getVersionId(), is(nullValue()));
  }

  @Test
  public void update_sendsVersionIdFromCurrentGlueTable_whenEnabledForHiveTable() {
    when(gluePartitionService.isSendVersionId()).thenReturn(true);
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResultWithVersion("7"));
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    GlueTableService.UpdateOutcome outcome = service.update(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.UpdateOutcome.UPDATED));
    verify(glueClient).getTable(any(GetTableRequest.class));
    verify(glueClient).updateTable(captor.capture());
    assertThat(captor.getValue().getVersionId(), is("7"));
  }

  @Test
  public void update_skipsVersionId_forIcebergTable_evenWhenEnabled() {
    when(gluePartitionService.isSendVersionId()).thenReturn(true);
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    // metadata_location present => Iceberg; no getTable, no versionId
    GlueTableService.UpdateOutcome outcome = service.update(
        hmsTable(LOCATION, LAST_DDL_TIME, "s3://bucket/test_table/metadata/v1.metadata.json"));

    assertThat(outcome, is(GlueTableService.UpdateOutcome.UPDATED));
    verify(glueClient, never()).getTable(any(GetTableRequest.class));
    verify(glueClient).updateTable(captor.capture());
    assertThat(captor.getValue().getVersionId(), is(nullValue()));
  }

  @Test
  public void update_fallsBackToUnconditionalUpdate_onConcurrentModification() {
    when(gluePartitionService.isSendVersionId()).thenReturn(true);
    when(glueClient.getTable(any(GetTableRequest.class))).thenReturn(glueTableResultWithVersion("7"));
    // Versioned write conflicts; the unconditional fallback (no versionId) then succeeds.
    when(glueClient.updateTable(any(UpdateTableRequest.class)))
        .thenThrow(new ConcurrentModificationException("conflict"))
        .thenReturn(new UpdateTableResult());
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    GlueTableService.UpdateOutcome outcome = service.update(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.UpdateOutcome.VERSION_CONFLICT_FALLBACK));
    // one versioned attempt (conflict) + one unconditional fallback; version read only once
    verify(glueClient).getTable(any(GetTableRequest.class));
    verify(glueClient, times(2)).updateTable(captor.capture());
    assertThat(captor.getValue().getVersionId(), is(nullValue()));
  }

  @Test
  public void update_degradesToUnconditionalUpdate_whenGetTableFailsWithServiceError() {
    when(gluePartitionService.isSendVersionId()).thenReturn(true);
    when(glueClient.getTable(any(GetTableRequest.class))).thenThrow(new OperationTimeoutException("throttled"));
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);

    GlueTableService.UpdateOutcome outcome = service.update(hmsTable(LOCATION, LAST_DDL_TIME, null));

    assertThat(outcome, is(GlueTableService.UpdateOutcome.VERSION_UNAVAILABLE_FALLBACK));
    verify(glueClient).getTable(any(GetTableRequest.class));
    verify(glueClient).updateTable(captor.capture());
    assertThat(captor.getValue().getVersionId(), is(nullValue()));
  }

  @Test(expected = EntityNotFoundException.class)
  public void update_propagatesEntityNotFound_whenTableMissing_soCallerCanCreate() {
    when(gluePartitionService.isSendVersionId()).thenReturn(true);
    when(glueClient.getTable(any(GetTableRequest.class))).thenThrow(new EntityNotFoundException("not found"));

    try {
      service.update(hmsTable(LOCATION, LAST_DDL_TIME, null));
    } finally {
      verify(glueClient, never()).updateTable(any(UpdateTableRequest.class));
    }
  }

  private GetTableResult glueTableResultWithVersion(String versionId) {
    return new GetTableResult().withTable(
        new com.amazonaws.services.glue.model.Table()
            .withName(TABLE_NAME)
            .withDatabaseName(DB_NAME)
            .withVersionId(versionId));
  }

  private Table hmsTable(String location, String lastDdlTime, String metadataLocation) {
    Table table = new Table();
    table.setTableName(TABLE_NAME);
    table.setDbName(DB_NAME);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    sd.setSerdeInfo(new SerDeInfo());
    table.setSd(sd);

    Map<String, String> params = new HashMap<>();
    if (lastDdlTime != null) {
      params.put("transient_lastDdlTime", lastDdlTime);
    }
    if (metadataLocation != null) {
      params.put("metadata_location", metadataLocation);
    }
    table.setParameters(params);
    return table;
  }

  private GetTableResult glueTableResult(String location, String lastDdlTime, String metadataLocation) {
    com.amazonaws.services.glue.model.StorageDescriptor sd = new com.amazonaws.services.glue.model.StorageDescriptor()
        .withLocation(location);

    Map<String, String> params = new HashMap<>();
    if (lastDdlTime != null) {
      params.put("transient_lastDdlTime", lastDdlTime);
    }
    if (metadataLocation != null) {
      params.put("metadata_location", metadataLocation);
    }

    return new GetTableResult().withTable(
        new com.amazonaws.services.glue.model.Table()
            .withName(TABLE_NAME)
            .withDatabaseName(DB_NAME)
            .withStorageDescriptor(sd)
            .withParameters(params));
  }

  private GetTableResult glueTableResultNullParams() {
    return new GetTableResult().withTable(
        new com.amazonaws.services.glue.model.Table()
            .withName(TABLE_NAME)
            .withDatabaseName(DB_NAME)
            .withParameters((Map<String, String>) null));
  }
}
