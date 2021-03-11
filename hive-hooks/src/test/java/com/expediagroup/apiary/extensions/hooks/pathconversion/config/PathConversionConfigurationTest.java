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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PathConversionConfigurationTest {

  @Mock private HiveConf hiveConf;

  private Properties properties;

  @Before
  public void init() {
    properties = new Properties();
    properties.setProperty(PathConversionConfiguration.PATH_REPLACEMENT_ENABLED, "true");
  }

  @Test
  public void shouldCheckHiveSiteForPathReplacementEnablement() {
    when(hiveConf.getAllProperties()).thenReturn(properties);
    PathConversionConfiguration conf = new PathConversionConfiguration(hiveConf);
    assertTrue(conf.isPathConversionEnabled());
  }

  @Test
  public void shouldProperlyInitializePathConversions() {
    String regexKey = format("%s.test", PathConversionConfiguration.PATH_REPLACEMENT_REGEX);
    String valueKey = format("%s.test", PathConversionConfiguration.PATH_REPLACEMENT_VALUES);
    properties.setProperty(regexKey, "s3a");
    properties.setProperty(valueKey, "s3");
    when(hiveConf.getAllProperties()).thenReturn(properties);
    PathConversionConfiguration conf = new PathConversionConfiguration(hiveConf);
    assertEquals(conf.getPathConversions().size(), 1);
  }

  @Test
  public void shouldSkipIfRegexUnset() {
    String valueKey = format("%s.test", PathConversionConfiguration.PATH_REPLACEMENT_VALUES);
    properties.setProperty(valueKey, "s3");
    when(hiveConf.getAllProperties()).thenReturn(properties);
    PathConversionConfiguration conf = new PathConversionConfiguration(hiveConf);
    assertEquals(conf.getPathConversions().size(), 0);
  }

  @Test
  public void shouldSkipIfValueUnset() {
    String regexKey = format("%s.test", PathConversionConfiguration.PATH_REPLACEMENT_REGEX);
    properties.setProperty(regexKey, "s3a");
    when(hiveConf.getAllProperties()).thenReturn(properties);
    PathConversionConfiguration conf = new PathConversionConfiguration(hiveConf);
    assertEquals(conf.getPathConversions().size(), 0);
  }
}
