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
package com.expediagroup.apiary.extensions.hooks.pathconversion.config;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.hive.conf.HiveConf;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;

import com.expediagroup.apiary.extensions.hooks.pathconversion.models.PathConversion;

@Slf4j
@Getter
public class Configuration {

  private static final String PATH_REPLACEMENT_PREFIX = "apiary.path.replacement";
  static final String PATH_REPLACEMENT_ENABLED = format("%s.enabled", PATH_REPLACEMENT_PREFIX);
  static final String PATH_REPLACEMENT_REGEX = format("%s.regex", PATH_REPLACEMENT_PREFIX);
  static final String PATH_REPLACEMENT_VALUES = format("%s.value", PATH_REPLACEMENT_PREFIX);
  private static final String PATH_REPLACEMENT_GROUPS = format("%s.capturegroups", PATH_REPLACEMENT_PREFIX);

  private final Properties properties;

  private final boolean pathConversionEnabled;
  private final List<PathConversion> pathConversions;

  public Configuration(HiveConf conf) {
    Properties props = conf.getAllProperties();

    if (Objects.isNull(props)) {
      log.warn("Could not load properties from HiveConf! Hive may be mis-configured.");
      props = new Properties();
    }

    properties = props;
    pathConversionEnabled = BooleanUtils.toBoolean(props.getProperty(PATH_REPLACEMENT_ENABLED, "false"));
    pathConversions = initializePathReplacements();
  }

  private List<PathConversion> initializePathReplacements() {
    List<PathConversion> pathConversions = new ArrayList<>();
    for (Object keyObj : properties.keySet()) {
      String curPropName = (String) keyObj;

      if (curPropName.startsWith(PATH_REPLACEMENT_REGEX)) {
        String valuePropName = curPropName.replace(PATH_REPLACEMENT_REGEX, PATH_REPLACEMENT_VALUES);
        String value = properties.getProperty(valuePropName);

        String captureGroupPropName = curPropName.replace(PATH_REPLACEMENT_REGEX, PATH_REPLACEMENT_GROUPS);
        List<Integer> captureGroups = getCaptureGroups(captureGroupPropName);

        if (Objects.isNull(value)) {
          log.warn("Non-existent value property for PathMatchProperty[{}]. " +
                  "This will not be replaced, please reconfigure Apiary Metastore Filter in hive-site.xml",
              curPropName);
          continue;
        }

        Pattern pattern = Pattern.compile(properties.getProperty(curPropName));
        pathConversions.add(new PathConversion(pattern, value, captureGroups));
        log.debug("Tracking PathMatchProperty[{}] for path conversion.", curPropName);
      }
    }
    return ImmutableList.copyOf(pathConversions);
  }

  private List<Integer> getCaptureGroups(String propertyName) {
    String captureGroups = properties.getProperty(propertyName, "1");
    return Arrays.stream(captureGroups.split(",")).map(Integer::parseInt).collect(Collectors.toList());
  }
}
