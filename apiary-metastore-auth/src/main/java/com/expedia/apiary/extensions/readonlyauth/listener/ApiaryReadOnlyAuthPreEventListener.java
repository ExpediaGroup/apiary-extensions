/**
 * Copyright (C) 2018 Expedia Inc.
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

package com.expedia.apiary.extensions.readonlyauth.listener;

import java.util.Set;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;

import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreReadTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAddIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreDropIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.PreReadDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * A MetaStorePreEventListener to authorize access using static database list.
 */
public class ApiaryReadOnlyAuthPreEventListener extends MetaStorePreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryReadOnlyAuthPreEventListener.class);

  private List<String> sharedDatabaseList = null;

  public ApiaryReadOnlyAuthPreEventListener(Configuration config) throws HiveException {
    super(config);

    String databaseNames = System.getenv("SHARED_HIVE_DB_NAMES");
    if (databaseNames == null || databaseNames.equals("")) {
      throw new IllegalArgumentException(
          "SHARED_HIVE_DB_NAMES System envrionment variable not defined");
    }
    sharedDatabaseList =  new ArrayList<String>(Arrays.asList(databaseNames.split(",")));

    log.debug("ApiaryReadOnlyAuthPreEventListener created");
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException, InvalidOperationException {

    Table table = null;
    Database db = null;
    String databaseName = null;

    switch (context.getEventType()) {
    case CREATE_TABLE:
      throw new InvalidOperationException("Create Table is disabled from read-only metastores.");
    case DROP_TABLE:
      throw new InvalidOperationException("Drop Table is disabled from read-only metastores.");
    case ALTER_TABLE:
      throw new InvalidOperationException("Alter Table is disabled from read-only metastores.");
    case READ_TABLE:
      table = ((PreReadTableEvent) context).getTable();
      databaseName = table.getDbName();
      if(!isAllowedDatabase(databaseName))
        throw new InvalidOperationException( databaseName + " Database is not in allowed list.");
      break;
    case ADD_PARTITION:
      throw new InvalidOperationException("Add Partition is disabled from read-only metastores.");
    case DROP_PARTITION:
      throw new InvalidOperationException("Drop Partition is disabled from read-only metastores.");
    case ALTER_PARTITION:
      throw new InvalidOperationException("Alter Partition is disabled from read-only metastores.");
    case ADD_INDEX:
      throw new InvalidOperationException("Add Index is disabled from read-only metastores.");
    case DROP_INDEX:
      throw new InvalidOperationException("Drop Index is disabled from read-only metastores.");
    case ALTER_INDEX:
      throw new InvalidOperationException("Alter Index is disabled from read-only metastores.");
    case READ_DATABASE:
      db = ((PreReadDatabaseEvent) context).getDatabase();
      databaseName = db.getName();
      if(!isAllowedDatabase(databaseName))
        throw new InvalidOperationException( databaseName + " Database is not in allowed list.");
      break;
    case CREATE_DATABASE:
      throw new InvalidOperationException("Create Database is disabled from read-only metastores.");
    case DROP_DATABASE:
      throw new InvalidOperationException("Drop Database is disabled from read-only metastores.");
    default:
      return;
    }

  }

  private boolean isAllowedDatabase(String dbName) {
    if(sharedDatabaseList.contains(dbName))
      return true;

    return false;
  }
}
