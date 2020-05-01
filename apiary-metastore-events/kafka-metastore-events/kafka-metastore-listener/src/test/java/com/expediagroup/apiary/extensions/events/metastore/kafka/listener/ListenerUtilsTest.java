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
package com.expediagroup.apiary.extensions.events.metastore.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.awaitility.Durations;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Counter;

import com.expediagroup.apiary.extensions.events.metastore.kafka.metrics.MetricsConstant;

public class ListenerUtilsTest {

  private TestAppender appender = new TestAppender();
  private HiveConf conf = new HiveConf();

  @Before
  public void init() throws Exception {
    MetricsFactory.close();
    MetricsFactory.init(conf);
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(appender);
    appender.clear();
  }

  @Test
  public void success() {
    ListenerUtils.success();
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    Counter counter = metrics.getMetricRegistry()
      .counter(MetricsConstant.LISTENER_SUCCESSES);
    await().atMost(Durations.FIVE_SECONDS).until(() -> counter.getCount() == 1L);
  }

  @Test
  public void error() {
    Exception e = new RuntimeException("ABC");
    ListenerUtils.error(e);
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    Counter counter = metrics.getMetricRegistry()
      .counter(MetricsConstant.LISTENER_FAILURES);

    await().atMost(Durations.FIVE_SECONDS).until(() -> counter.getCount() == 1L);
    // We want to make sure these keywords are logged
    List<LoggingEvent> events = appender.getEvents();
    assertThat(events).hasSize(1);
    assertThat(events).extracting("renderedMessage").containsExactly("Error in Kafka Listener");
  }

}
