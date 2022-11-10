package com.expediagroup.apiary.extensions.gluesync.listener;

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.After;
import org.junit.Before;

import com.expediagroup.egdl.icelander.test.LocalHiveMetastore;

public class HiveMetastoreTestBase {
  protected static final String DB_NAME = "test";
  protected static HiveMetaStoreClient metastoreClient;
  protected static HiveCatalog catalog;
  protected static HiveConf hiveConf;
  protected static LocalHiveMetastore metastore;

  public HiveMetastoreTestBase() {
  }

  @Before
  public void startMetastore() throws Exception {
    metastore = new LocalHiveMetastore();
    metastore.start();
    hiveConf = metastore.hiveConf();
    metastoreClient = new HiveMetaStoreClient(hiveConf);
    String dbPath = metastore.getDatabasePath("test");
    Database db = new Database("test", "description", dbPath, new HashMap());
    metastoreClient.createDatabase(db);
    catalog = new HiveCatalog();
    catalog.setConf(hiveConf);
    catalog.initialize("hive", new HashMap());
  }

  @After
  public void stopMetastore() {
    catalog = null;
    metastoreClient.close();
    metastoreClient = null;
    metastore.stop();
    metastore = null;
  }
}
