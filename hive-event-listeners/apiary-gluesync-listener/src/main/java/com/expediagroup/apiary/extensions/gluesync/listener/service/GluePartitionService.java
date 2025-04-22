package com.expediagroup.apiary.extensions.gluesync.listener.service;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreatePartitionRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;

public class GluePartitionService {
  private static final Logger log = LoggerFactory.getLogger(GluePartitionService.class);

  private final AWSGlue glueClient;
  private final HiveToGlueTransformer transformer;

  public GluePartitionService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.transformer = new HiveToGlueTransformer(gluePrefix);
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table, Partition partition) {
    CreatePartitionRequest createPartitionRequest = new CreatePartitionRequest()
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    glueClient.createPartition(createPartitionRequest);
    log.debug("{} partition created in glue catalog", partition);
  }

  public void update(Table table,Partition partition) {
    UpdatePartitionRequest updatePartitionRequest = new UpdatePartitionRequest()
        .withPartitionValueList(transformer.transformPartition(partition).getValues())
        .withPartitionInput(transformer.transformPartition(partition))
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    glueClient.updatePartition(updatePartitionRequest);
    log.debug("{} partition updated in glue catalog", partition);
  }

  public void delete(Table table, Partition partition) {
    DeletePartitionRequest deletePartitionRequest = new DeletePartitionRequest()
        .withPartitionValues(transformer.transformPartition(partition).getValues())
        .withDatabaseName(transformer.glueDbName(table))
        .withTableName(table.getTableName());
    glueClient.deletePartition(deletePartitionRequest);
    log.debug("{} partition deleted from glue catalog", partition);
  }


}
