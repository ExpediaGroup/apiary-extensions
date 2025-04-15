package com.expediagroup.apiary.extensions.gluesync.listener.service;

import static com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.MANAGED_BY_GLUESYNC_KEY;
import static com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.MANAGED_BY_GLUESYNC_VALUE;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;

public class GlueTableService {
  private static final Logger log = LoggerFactory.getLogger(GlueTableService.class);

  private final AWSGlue glueClient;
  private final String gluePrefix;
  private final HiveToGlueTransformer transformer;

  public GlueTableService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.gluePrefix = gluePrefix;
    this.transformer = new HiveToGlueTransformer();
    log.debug("ApiaryGlueSync created");
  }

  public void create(Table table) {
    CreateTableRequest createTableRequest = new CreateTableRequest()
        .withTableInput(transformer.transformTable(table))
        .withDatabaseName(transformer.glueDbName(table));
    glueClient.createTable(createTableRequest);
    log.info(table + " table created in glue catalog");
  }

  public void update(Database database) {
    // ...
  }

  public void delete(Database database) {
    // ...
  }


}
