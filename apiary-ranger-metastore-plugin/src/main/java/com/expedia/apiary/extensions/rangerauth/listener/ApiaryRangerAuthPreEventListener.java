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

package com.expedia.apiary.extensions.rangerauth.listener;

import java.util.Set;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.shims.Utils;
import com.google.common.collect.Sets;

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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;

import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;

enum HiveAccessType {
  NONE,
  CREATE,
  ALTER,
  DROP,
  INDEX,
  LOCK,
  SELECT,
  UPDATE,
  USE,
  READ,
  WRITE,
  ALL,
  ADMIN
};

/**
 * A MetaStorePreEventListener to authorize using Ranger.
 */
public class ApiaryRangerAuthPreEventListener extends MetaStorePreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryRangerAuthPreEventListener.class);

  private RangerBasePlugin plugin = null;

  public ApiaryRangerAuthPreEventListener(Configuration config) throws HiveException {
    super(config);
    plugin = new RangerBasePlugin("hive", "metastore");
    plugin.init();
    plugin.setResultProcessor(new RangerDefaultAuditHandler());
    log.debug("ApiaryRangerAuthPreEventListener created");
  }

  public ApiaryRangerAuthPreEventListener(Configuration config, RangerBasePlugin plugin) throws HiveException {
    super(config);
    this.plugin = plugin;
    log.debug("ApiaryRangerAuthPreEventListener created");
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException, InvalidOperationException {

    String user = null;
    Set<String> groups = null;

    try {
      UserGroupInformation ugi = Utils.getUGI();
      user = ugi.getUserName();
      groups = Sets.newHashSet(ugi.getGroupNames());
    } catch (Exception e) {
      throw new InvalidOperationException("Unable to read user/group information: " + e);
    }

    String operationName = null;
    HiveAccessType accessType = null;
    Table table = null;
    Database db = null;
    String databaseName = null;
    String tableName = null;
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();

    switch (context.getEventType()) {
    case CREATE_TABLE:
      table = ((PreCreateTableEvent) context).getTable();
      operationName = HiveOperationType.CREATETABLE.name();
      accessType = HiveAccessType.CREATE;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case DROP_TABLE:
      table = ((PreDropTableEvent) context).getTable();
      operationName = HiveOperationType.DROPTABLE.name();
      accessType = HiveAccessType.DROP;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case ALTER_TABLE:
      table = ((PreAlterTableEvent) context).getOldTable();
      operationName = "ALTERTABLE";
      accessType = HiveAccessType.ALTER;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case READ_TABLE:
      table = ((PreReadTableEvent) context).getTable();
      operationName = HiveOperationType.QUERY.name();
      accessType = HiveAccessType.SELECT;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case ADD_PARTITION:
      table = ((PreAddPartitionEvent) context).getTable();
      operationName = "ADDPARTITION";
      accessType = HiveAccessType.ALTER;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case DROP_PARTITION:
      table = ((PreDropPartitionEvent) context).getTable();
      operationName = "DROPPARTITION";
      accessType = HiveAccessType.ALTER;
      resource.setValue("database", table.getDbName());
      resource.setValue("table", table.getTableName());
      break;
    case ALTER_PARTITION:
      databaseName = ((PreAlterPartitionEvent) context).getDbName();
      tableName = ((PreAlterPartitionEvent) context).getTableName();
      accessType = HiveAccessType.ALTER;
      operationName = "ALTERPARTITION";
      resource.setValue("database", databaseName);
      resource.setValue("table", tableName);
      break;
    case ADD_INDEX:
      databaseName = ((PreAddIndexEvent) context).getIndex().getDbName();
      tableName = ((PreAddIndexEvent) context).getIndex().getOrigTableName();
      operationName = "ADDINDEX";
      accessType = HiveAccessType.CREATE;
      resource.setValue("database", databaseName);
      resource.setValue("table", tableName);
      break;
    case DROP_INDEX:
      operationName = "DROPINDEX";
      accessType = HiveAccessType.DROP;
      databaseName = ((PreDropIndexEvent) context).getIndex().getDbName();
      tableName = ((PreDropIndexEvent) context).getIndex().getOrigTableName();
      resource.setValue("database", databaseName);
      resource.setValue("table", tableName);
      break;
    case ALTER_INDEX:
      operationName = "ALTERINDEX";
      accessType = HiveAccessType.ALTER;
      databaseName = ((PreAlterIndexEvent) context).getOldIndex().getDbName();
      tableName = ((PreAlterIndexEvent) context).getOldIndex().getOrigTableName();
      resource.setValue("database", databaseName);
      resource.setValue("table", tableName);
      break;
    case READ_DATABASE:
      db = ((PreReadDatabaseEvent) context).getDatabase();
      operationName = HiveOperationType.QUERY.name();
      accessType = HiveAccessType.SELECT;
      resource.setValue("database", db.getName());
      break;
    case CREATE_DATABASE:
      db = ((PreCreateDatabaseEvent) context).getDatabase();
      operationName = HiveOperationType.CREATEDATABASE.name();
      accessType = HiveAccessType.CREATE;
      resource.setValue("database", db.getName());
      break;
    case DROP_DATABASE:
      db = ((PreDropDatabaseEvent) context).getDatabase();
      operationName = HiveOperationType.DROPDATABASE.name();
      accessType = HiveAccessType.DROP;
      resource.setValue("database", db.getName());
      break;
    default:
      return;
    }

    resource.setServiceDef(plugin.getServiceDef());
    RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, accessType.name().toLowerCase(), user,
        groups);
    request.setAccessTime(new Date());
    request.setAction(operationName);

    RangerAccessResult result = plugin.isAccessAllowed(request);
    if (result == null) {
      throw new InvalidOperationException("Permission denied: unable to evaluate ranger policy");
    }
    if (!result.getIsAllowed()) {
      String path = resource.getAsString();
      throw new InvalidOperationException(String
          .format("Permission denied: user [%s] does not have [%s] privilege on [%s]", user,
              accessType.name().toLowerCase(), path));
    }

  }

}
