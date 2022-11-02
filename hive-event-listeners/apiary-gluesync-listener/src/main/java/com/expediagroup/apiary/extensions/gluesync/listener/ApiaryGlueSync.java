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
package com.expediagroup.apiary.extensions.gluesync.listener;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.WebIdentityTokenCredentialsProvider;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.AlreadyExistsException;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

public class ApiaryGlueSync extends MetaStoreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryGlueSync.class);

  private static final String MANAGED_BY_GLUESYNC_KEY = "managed-by";
  private static final String MANAGED_BY_GLUESYNC_VALUE = "apiary-glue-sync";
  private final AWSGlue glueClient;
  private final String gluePrefix;

  public ApiaryGlueSync(Configuration config) {
    super(config);
    AWSGlueClientBuilder glueBuilder = AWSGlueClientBuilder.standard().withRegion(System.getenv("AWS_REGION"));
    if (System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != null && !System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE").isEmpty()) {
      glueBuilder.withCredentials(WebIdentityTokenCredentialsProvider.create());
    }
    glueClient = glueBuilder.build();
    gluePrefix = System.getenv("GLUE_PREFIX");
    log.debug("ApiaryGlueSync created");
  }

  public ApiaryGlueSync(Configuration config, AWSGlue glueClient, String gluePrefix) {
    super(config);
    this.glueClient = glueClient;
    this.gluePrefix = gluePrefix;
    log.debug("ApiaryGlueSync created");
  }

  @Override
  public void onCreateDatabase(CreateDatabaseEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Database database = event.getDatabase();
    try {
      CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest()
          .withDatabaseInput(transformDatabase(database));
      glueClient.createDatabase(createDatabaseRequest);
      log.info(database + " database created in glue catalog");
    } catch (AlreadyExistsException e) {
      log.info(database + " database already exists in glue, updating....");
      UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest()
          .withName(glueDbName(database.getName()))
          .withDatabaseInput(transformDatabase(database));
      glueClient.updateDatabase(updateDatabaseRequest);
      log.info(database + " database updated in glue catalog");
    }
  }

  @Override
  public void onDropDatabase(DropDatabaseEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Database database = event.getDatabase();
    com.amazonaws.services.glue.model.Database glueDb = glueClient.getDatabase(
        new GetDatabaseRequest().withName(glueDbName(database.getName()))).getDatabase();
    if (glueDb != null && glueDb.getParameters() != null) {
      String createdByProperty = glueDb.getParameters().get(MANAGED_BY_GLUESYNC_KEY);
      if (createdByProperty != null && createdByProperty.equals(MANAGED_BY_GLUESYNC_VALUE)) {
        try {
          DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest()
              .withName(glueDbName(database.getName()));
          glueClient.deleteDatabase(deleteDatabaseRequest);
          log.info(database + " database deleted from glue catalog");
          return;
        } catch (EntityNotFoundException e) {
          log.info(database + " database doesn't exist in glue catalog");
        }
      }
    }
    log.info("{} database not created by {}, will not be deleted from glue catalog", database,
        MANAGED_BY_GLUESYNC_VALUE);
  }

  @Override
  public void onCreateTable(CreateTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    try {
      CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.createTable(createTableRequest);
      log.info(table + " table created in glue catalog");
    } catch (AlreadyExistsException e) {
      log.info(table + " table already exists in glue, updating....");
      UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.updateTable(updateTableRequest);
      log.info(table + " table updated in glue catalog");
    }
  }

  @Override
  public void onDropTable(DropTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    try {
      DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
          .withName(table.getTableName())
          .withDatabaseName(glueDbName(table));
      glueClient.deleteTable(deleteTableRequest);
      log.info(table + " table deleted from glue catalog");
    } catch (EntityNotFoundException e) {
      log.info(table + " table doesn't exist in glue catalog");
    }
  }

  @Override
  public void onAlterTable(AlterTableEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getNewTable();
    try {
      UpdateTableRequest updateTableRequest = new UpdateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.updateTable(updateTableRequest);
      log.info(table + " table updated in glue catalog");
    } catch (EntityNotFoundException e) {
      log.info(table + " table doesn't exist in glue, creating....");
      CreateTableRequest createTableRequest = new CreateTableRequest()
          .withTableInput(transformTable(table))
          .withDatabaseName(glueDbName(table));
      glueClient.createTable(createTableRequest);
      log.info(table + " table created in glue catalog");
    }
  }

  @Override
  public void onAddPartition(AddPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
            .withPartitionInput(transformPartition(partition))
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.createPartition(createPartitionRequest);
      } catch (AlreadyExistsException e) {
        UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
            .withPartitionValueList(transformPartition(partition).getValues())
            .withPartitionInput(transformPartition(partition))
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.updatePartition(updatePartitionRequest);
      }
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Iterator<Partition> partitions = event.getPartitionIterator();
    while (partitions.hasNext()) {
      Partition partition = partitions.next();
      try {
        DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest()
            .withPartitionValues(transformPartition(partition).getValues())
            .withDatabaseName(glueDbName(table))
            .withTableName(table.getTableName());
        glueClient.deletePartition(deletePartitionRequest);
      } catch (EntityNotFoundException ignored) {}
    }
  }

  @Override
  public void onAlterPartition(AlterPartitionEvent event) {
    if (!event.getStatus()) {
      return;
    }
    Table table = event.getTable();
    Partition partition = event.getNewPartition();
    try {
      UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
          .withPartitionValueList(transformPartition(partition).getValues())
          .withPartitionInput(transformPartition(partition))
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName());
      glueClient.updatePartition(updatePartitionRequest);
    } catch (EntityNotFoundException e) {
      CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
          .withPartitionInput(transformPartition(partition))
          .withDatabaseName(glueDbName(table))
          .withTableName(table.getTableName());
      glueClient.createPartition(createPartitionRequest);
    }
  }

  DatabaseInput transformDatabase(Database database) {
    Map<String, String> params = database.getParameters();
    if (params == null) {
      params = new HashMap<>();
    }
    params.put(MANAGED_BY_GLUESYNC_KEY, MANAGED_BY_GLUESYNC_VALUE);
    return new DatabaseInput().withName(glueDbName(database.getName()))
        .withParameters(params)
        .withDescription(database.getDescription())
        .withLocationUri(database.getLocationUri());
  }

  TableInput transformTable(final Table table) {
    final Date date = convertTableDate(table.getLastAccessTime());

    List<Column> partitionKeys = extractColumns(table.getPartitionKeys());

    final org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = table.getSd();
    final List<Column> columns = extractColumns(storageDescriptor.getCols());

    final SerDeInfo glueSerde = new SerDeInfo()
        .withName(storageDescriptor.getSerdeInfo().getName())
        .withParameters(storageDescriptor.getSerdeInfo().getParameters())
        .withSerializationLibrary(storageDescriptor.getSerdeInfo().getSerializationLib());

    final List<Order> sortOrders = extractSortOrders(storageDescriptor.getSortCols());

    final StorageDescriptor sd = new StorageDescriptor()
        .withBucketColumns(storageDescriptor.getBucketCols())
        .withColumns(columns)
        .withCompressed(storageDescriptor.isCompressed())
        .withInputFormat(storageDescriptor.getInputFormat())
        .withLocation(storageDescriptor.getLocation())
        .withNumberOfBuckets(storageDescriptor.getNumBuckets())
        .withOutputFormat(storageDescriptor.getOutputFormat())
        .withParameters(storageDescriptor.getParameters())
        .withSerdeInfo(glueSerde)
        .withSortColumns(sortOrders)
        .withStoredAsSubDirectories(storageDescriptor.isStoredAsSubDirectories());

    return new TableInput()
        .withName(table.getTableName())
        .withLastAccessTime(date)
        .withOwner(table.getOwner())
        .withParameters(table.getParameters())
        .withPartitionKeys(partitionKeys)
        .withRetention(table.getRetention())
        .withStorageDescriptor(sd)
        .withTableType(table.getTableType());
  }

  PartitionInput transformPartition(final Partition partition) {
    final Date date = convertTableDate(partition.getLastAccessTime());

    final org.apache.hadoop.hive.metastore.api.StorageDescriptor storageDescriptor = partition.getSd();
    final Collection<Column> columns = extractColumns(storageDescriptor.getCols());

    final SerDeInfo glueSerde = new SerDeInfo()
        .withName(storageDescriptor.getSerdeInfo().getName())
        .withParameters(storageDescriptor.getSerdeInfo().getParameters())
        .withSerializationLibrary(storageDescriptor.getSerdeInfo().getSerializationLib());

    final List<Order> sortOrders = extractSortOrders(storageDescriptor.getSortCols());

    final StorageDescriptor sd = new StorageDescriptor()
        .withBucketColumns(storageDescriptor.getBucketCols())
        .withColumns(columns)
        .withCompressed(storageDescriptor.isCompressed())
        .withInputFormat(storageDescriptor.getInputFormat())
        .withLocation(storageDescriptor.getLocation())
        .withNumberOfBuckets(storageDescriptor.getNumBuckets())
        .withOutputFormat(storageDescriptor.getOutputFormat())
        .withParameters(storageDescriptor.getParameters())
        .withSerdeInfo(glueSerde)
        .withSortColumns(sortOrders)
        .withStoredAsSubDirectories(storageDescriptor.isStoredAsSubDirectories());

    return new PartitionInput()
        .withLastAccessTime(date)
        .withParameters(partition.getParameters())
        .withStorageDescriptor(sd)
        .withValues(partition.getValues());
  }

  private List<Order> extractSortOrders(final List<org.apache.hadoop.hive.metastore.api.Order> hiveOrders) {
    final List<Order> sortOrders = new ArrayList<>();
    for (final org.apache.hadoop.hive.metastore.api.Order hiveOrder : hiveOrders) {
      final Order order = new Order().withSortOrder(hiveOrder.getOrder()).withColumn(hiveOrder.getCol());
      sortOrders.add(order);
    }
    return sortOrders;
  }

  private List<Column> extractColumns(final List<FieldSchema> colList) {
    final List<Column> columns = new ArrayList<>();
    if (colList == null) {
      return columns;
    }

    for (final FieldSchema fieldSchema : colList) {
      final Column col = new Column()
          .withName(fieldSchema.getName())
          .withType(fieldSchema.getType())
          .withComment(fieldSchema.getComment());

      columns.add(col);
    }
    return columns;
  }

  private Date convertTableDate(final Integer lastAccessTime) {
    if (lastAccessTime == 0) {
      return null;
    }
    try {
      final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
      return dateFormat.parse(lastAccessTime.toString());
    } catch (Exception e) {
      log.error("Error formatting table date", e);
    }
    return null;
  }

  private String glueDbName(String dbName) {
    return (gluePrefix == null) ? dbName : gluePrefix + dbName;
  }

  private String glueDbName(Table table) {
    return glueDbName(table.getDbName());
  }
}
