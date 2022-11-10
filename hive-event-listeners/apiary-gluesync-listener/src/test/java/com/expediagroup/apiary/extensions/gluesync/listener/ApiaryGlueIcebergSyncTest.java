package com.expediagroup.apiary.extensions.gluesync.listener;

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateTableResult;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryGlueIcebergSyncTest extends HiveMetastoreTestBase {
  private static final String TABLE_NAME = "table";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema SCHEMA = new Schema(required(1, "string", Types.StringType.get()));
  private static final PartitionSpec PARTITION_SPEC = builderFor(SCHEMA).identity("string").build();

  @Mock
  private AWSGlue glueClient;
  @Mock
  private Configuration configuration;
  @Mock
  private CreateTableResult createTableResult;

  @Captor
  private ArgumentCaptor<CreateTableRequest> tableRequestCaptor;

  private final String gluePrefix = "test_";
  private ApiaryGlueSync glueSync;

  @Before
  public void setup() {
    glueSync = new ApiaryGlueSync(configuration, glueClient, gluePrefix);
    when(glueClient.createTable(any(CreateTableRequest.class))).thenReturn(createTableResult);
  }

  @Test
  public void onCreateTable() throws TException {
    CreateTableEvent event = mock(CreateTableEvent.class);
    when(event.getStatus()).thenReturn(true);

    catalog.createTable(TABLE_IDENTIFIER, SCHEMA, PARTITION_SPEC);
    Table icebergTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);
    catalog.dropTable(TABLE_IDENTIFIER);
    when(event.getTable()).thenReturn(icebergTable);

    glueSync.onCreateTable(event);

    verify(glueClient).createTable(tableRequestCaptor.capture());
    CreateTableRequest createTableRequest = tableRequestCaptor.getValue();

    assertThat(createTableRequest.getDatabaseName(), is(gluePrefix + DB_NAME));
    assertThat(createTableRequest.getTableInput().getName(), is(TABLE_NAME));
    assertThat(createTableRequest.getTableInput().getPartitionKeys().size(), is(0));
  }
}
