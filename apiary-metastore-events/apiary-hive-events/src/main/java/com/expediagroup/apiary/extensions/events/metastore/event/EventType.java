/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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

/**
 * To make processing event in the receiver easier.
 */
public enum EventType {
  ON_CREATE_TABLE(ApiaryCreateTableEvent.class),
  ON_ALTER_TABLE(ApiaryAlterTableEvent.class),
  ON_DROP_TABLE(ApiaryDropTableEvent.class),
  ON_ADD_PARTITION(ApiaryAddPartitionEvent.class),
  ON_ALTER_PARTITION(ApiaryAlterPartitionEvent.class),
  ON_DROP_PARTITION(ApiaryDropPartitionEvent.class),
  ON_INSERT(ApiaryInsertEvent.class);

  private final Class<? extends ApiaryListenerEvent> eventClass;

  private EventType(Class<? extends ApiaryListenerEvent> eventClass) {
    if (eventClass == null) {
      throw new NullPointerException("Parameter eventClass is required");
    }
    this.eventClass = eventClass;
  }

  public Class<? extends ApiaryListenerEvent> eventClass() {
    return eventClass;
  }

  public static EventType forClass(Class<? extends ApiaryListenerEvent> clazz) {
    for (EventType e : values()) {
      if (e.eventClass().equals(clazz)) {
        return e;
      }
    }
    throw new IllegalArgumentException("EventType not found for class " + clazz.getName());
  }

}
