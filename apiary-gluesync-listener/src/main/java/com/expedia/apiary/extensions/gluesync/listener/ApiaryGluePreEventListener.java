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

package com.expedia.apiary.extensions.gluesync.listener;

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * A MetaStorePreEventListener that prevents actions not compatible with glue catalog.
 */

public class ApiaryGluePreEventListener extends MetaStorePreEventListener {

  public ApiaryGluePreEventListener(Configuration config) throws HiveException {
    super(config);
    System.out.println(" ApiaryGluePreEventListener created ");
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException, NoSuchObjectException, InvalidOperationException {

    switch (context.getEventType()) {
    case CREATE_DATABASE:
      throw new InvalidOperationException("Create Database is disabled when glue sync in enabled.");
    case DROP_DATABASE:
      throw new InvalidOperationException("Drop Database is disabled when glue sync in enabled.");
    case ALTER_TABLE:
      PreAlterTableEvent event = (PreAlterTableEvent)context;
      if(!(event.getOldTable().getTableName().equals(event.getNewTable().getTableName())))
          throw new InvalidOperationException("Rename Table is not allowed when glue sync is enabled");
      break;
    case ALTER_PARTITION:
      PreAlterPartitionEvent event2 = (PreAlterPartitionEvent)context;
      List<String> oldPartValues = event2.getOldPartVals();
      List<String> newPartValues = event2.getNewPartition().getValues();
      if(oldPartValues!=null && !(newPartValues.equals(oldPartValues)))
          throw new InvalidOperationException("Rename Partition is not allowed when glue sync is enabled");
      break;
    default:
      break;
    }

  }

}
