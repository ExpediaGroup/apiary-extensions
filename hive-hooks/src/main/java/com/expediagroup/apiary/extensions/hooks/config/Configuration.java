/**
 * Copyright (C) 2018-2021 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.hooks.config;

import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public abstract class Configuration {

  protected final Properties properties;

  public Configuration(HiveConf conf) {
    Properties hiveProperties = conf.getAllProperties();

    if (hiveProperties == null) {
      log.warn("Could not load properties from HiveConf! Hive may be mis-configured.");
      hiveProperties = new Properties();
    }

    properties = hiveProperties;
  }
}
