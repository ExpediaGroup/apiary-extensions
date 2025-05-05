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

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

/**
 * A MetaStorePreEventListener that prevents actions not compatible with glue catalog.
 */

public class ApiaryGluePreEventListener extends MetaStorePreEventListener {

  private static final Logger log = LoggerFactory.getLogger(ApiaryGluePreEventListener.class);
  private final IsIcebergTablePredicate isIcebergPredicate = new IsIcebergTablePredicate();
  private final String enableHiveToGlueRenameOperation;

  public ApiaryGluePreEventListener(Configuration config) {
    super(config);
    enableHiveToGlueRenameOperation = System.getenv("ENABLE_HIVE_TO_GLUE_RENAME_OPERATION");
    log.debug("ApiaryGluePreEventListener created");
  }

  @Override
  public void onEvent(PreEventContext context) throws InvalidOperationException {
    if (Objects.requireNonNull(context.getEventType()) != PreEventContext.PreEventType.ALTER_PARTITION &&
        Objects.requireNonNull(context.getEventType()) != PreEventContext.PreEventType.ALTER_TABLE) {
      return;
    }
    switch (context.getEventType()) {
    case ALTER_TABLE:
      PreAlterTableEvent tableEvent = (PreAlterTableEvent) context;
      // Glue support iceberg renames, if Hive, check if rename workaround is enabled
      if (hiveToGlueRenameOperationEnabled() || isIceberg(tableEvent)) {
        break;
      }
      if (isRenameOperation(tableEvent)) {
        throw new InvalidOperationException("Rename Table is not allowed when glue sync is enabled");
      }
      break;
    case ALTER_PARTITION:
      PreAlterPartitionEvent partitionEvent = (PreAlterPartitionEvent) context;
      List<String> oldPartValues = partitionEvent.getOldPartVals();
      List<String> newPartValues = partitionEvent.getNewPartition().getValues();
      if (oldPartValues != null && !(newPartValues.equals(oldPartValues))) {
        throw new InvalidOperationException("Rename Partition is not allowed when glue sync is enabled");
      }
      break;
    default:
      break;
    }
  }

  private boolean isRenameOperation(PreAlterTableEvent tableEvent) {
    return !(tableEvent.getOldTable().getTableName().equals(tableEvent.getNewTable().getTableName()));
  }

  private boolean isIceberg(PreAlterTableEvent tableEvent) {
    return isIcebergPredicate.test(tableEvent.getOldTable().getParameters());
  }

  private boolean hiveToGlueRenameOperationEnabled() {
    return enableHiveToGlueRenameOperation != null &&
        (Boolean.parseBoolean(enableHiveToGlueRenameOperation) ||
            enableHiveToGlueRenameOperation.equalsIgnoreCase("1"));
  }
}
