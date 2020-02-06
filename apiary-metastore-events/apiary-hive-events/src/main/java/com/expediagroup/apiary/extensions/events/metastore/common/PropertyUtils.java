/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.common;

import org.apache.hadoop.conf.Configuration;

public class PropertyUtils {

  private PropertyUtils() {}

  public static String stringProperty(Configuration conf, Property property) {
    String value = System.getenv(property.environmentKey());
    if (value == null) {
      return conf.get(property.hadoopConfKey(), (String) property.defaultValue());
    }
    return value;
  }

  public static boolean booleanProperty(Configuration conf, Property property) {
    String value = System.getenv(property.environmentKey());
    if (value == null) {
      return conf.getBoolean(property.hadoopConfKey(), (boolean) property.defaultValue());
    }
    return Boolean.parseBoolean(value);
  }

  public static int intProperty(Configuration conf, Property property) {
    String value = System.getenv(property.environmentKey());
    if (value == null) {
      return conf.getInt(property.hadoopConfKey(), (int) property.defaultValue());
    }
    return Integer.parseInt(value);
  }

  public static long longProperty(Configuration conf, Property property) {
    String value = System.getenv(property.environmentKey());
    if (value == null) {
      return conf.getLong(property.hadoopConfKey(), (long) property.defaultValue());
    }
    return Long.parseLong(value);
  }

}
