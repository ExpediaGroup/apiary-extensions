package com.expediagroup.apiary.extensions.gluesync.listener.service;

import static com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.MANAGED_BY_GLUESYNC_KEY;
import static com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.MANAGED_BY_GLUESYNC_VALUE;

import org.apache.hadoop.hive.metastore.api.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;

public class GlueDatabaseService {
  private static final Logger log = LoggerFactory.getLogger(GlueDatabaseService.class);

  private final AWSGlue glueClient;
  private final String gluePrefix;
  private final HiveToGlueTransformer transformer;

  public GlueDatabaseService(AWSGlue glueClient, String gluePrefix) {
    this.glueClient = glueClient;
    this.gluePrefix = gluePrefix;
    this.transformer = new HiveToGlueTransformer();
    log.debug("ApiaryGlueSync created");
  }

  public void create(Database database) {
    CreateDatabaseRequest createDatabaseRequest = new CreateDatabaseRequest()
        .withDatabaseInput(transformer.transformDatabase(database));
    glueClient.createDatabase(createDatabaseRequest);
    log.info(database + " database created in glue catalog");
  }

  public void update(Database database) {
    UpdateDatabaseRequest updateDatabaseRequest = new UpdateDatabaseRequest()
        .withName(transformer.glueDbName(database.getName()))
        .withDatabaseInput(transformer.transformDatabase(database));
    glueClient.updateDatabase(updateDatabaseRequest);
    log.info(database + " database updated in glue catalog");
  }

  public void delete(Database database) {
    com.amazonaws.services.glue.model.Database glueDb = glueClient.getDatabase(
        new GetDatabaseRequest().withName(transformer.glueDbName(database.getName()))).getDatabase();
    if (glueDb != null && glueDb.getParameters() != null) {
      String createdByProperty = glueDb.getParameters().get(MANAGED_BY_GLUESYNC_KEY);
      if (createdByProperty != null && createdByProperty.equals(MANAGED_BY_GLUESYNC_VALUE)) {
        try {
          DeleteDatabaseRequest deleteDatabaseRequest = new DeleteDatabaseRequest()
              .withName(transformer.glueDbName(database.getName()));
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


}
