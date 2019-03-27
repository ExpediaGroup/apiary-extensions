/**
 * Copyright (C) 2018-2019 Expedia Inc.
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
package com.expedia.apiary.extensions.receiver.common.event;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;

public abstract class ListenerEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * Status of the event in {@link ListenerEvent}
   */
  private final boolean status = true;

  /**
   * Unmodifiable parameters in {@link ListenerEvent}
   */
  private final Map<String, String> tableParameters = new HashMap<>();

  /**
   * Properties passed by the client, to be used in execution hooks. EnvironmentContext in {@link ListenerEvent}
   */
  private EnvironmentContext environmentContext;

  protected ListenerEvent() {}

  public EventType getEventType() {
    return EventType.forClass(this.getClass());
  }

  public abstract String getDbName();

  public abstract String getTableName();

  public String getQualifiedTableName() {
    return String.join(".", getDbName(), getTableName());
  }

  public boolean getStatus() {
    return status;
  }

  public Map<String, String> getTableParameters() {
    return tableParameters;
  }

  public EnvironmentContext getEnvironmentContext() {
    return environmentContext;
  }

  @Override
  public String toString() {
    return ReflectionToStringBuilder.toString(this);
  }
}
