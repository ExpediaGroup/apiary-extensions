/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.event;


import static com.expediagroup.apiary.extensions.events.metastore.event.CustomEventParameters.HIVE_VERSION;

import java.util.Map;

import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hive.common.util.HiveVersionInfo;

public class ApiaryListenerEventFactory {

  public ApiaryCreateTableEvent create(CreateTableEvent event) {
    return new ApiaryCreateTableEvent(addParams(event));
  }

  public ApiaryAlterTableEvent create(AlterTableEvent event) {
    return new ApiaryAlterTableEvent(addParams(event));
  }

  public ApiaryDropTableEvent create(DropTableEvent event) {
    return new ApiaryDropTableEvent(addParams(event));
  }

  public ApiaryAddPartitionEvent create(AddPartitionEvent event) {
    return new ApiaryAddPartitionEvent(addParams(event));
  }

  public ApiaryAlterPartitionEvent create(AlterPartitionEvent event) {
    return new ApiaryAlterPartitionEvent(addParams(event));
  }

  public ApiaryDropPartitionEvent create(DropPartitionEvent event) {
    return new ApiaryDropPartitionEvent(addParams(event));
  }

  public ApiaryInsertEvent create(InsertEvent event) {
    return new ApiaryInsertEvent(addParams(event));
  }

  private <T extends ListenerEvent> T addParams(T event) {
    Map<String, String> eventParams = event.getParameters();
    if (eventParams != null && !eventParams.containsKey(HIVE_VERSION.varname())) {
      event.putParameter(HIVE_VERSION.varname(), HiveVersionInfo.getVersion());
    }

    return event;
  }

}
