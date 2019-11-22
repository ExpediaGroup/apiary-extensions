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
package com.expediagroup.apiary.extensions.events.metastore.kafka.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

public class HiveMetricsHelperTest {

  private HiveConf conf = new HiveConf();

  @Before
  public void setup() throws Exception {
    MetricsFactory.close();
  }

  @Test
  public void incrementCounter() throws Exception {
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, "org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics");
    MetricsFactory.init(conf);
    assertThat(HiveMetricsHelper.incrementCounter("name")).get().isEqualTo(1L);
  }

  @Test
  public void nullMetricsClass() {
    assertThat(HiveMetricsHelper.incrementCounter("name")).isNotPresent();
  }

  @Test
  public void metricsThrowsException() throws Exception {
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, "com.expediagroup.apiary.extensions.events.metastore.kafka.metrics."
      + "ExceptionMetrics");
    MetricsFactory.init(conf);
    assertThat(HiveMetricsHelper.incrementCounter("name")).isNotPresent();
  }

}
