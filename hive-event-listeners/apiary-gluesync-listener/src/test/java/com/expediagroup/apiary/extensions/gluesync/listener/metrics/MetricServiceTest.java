/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.Test;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

public class MetricServiceTest {

  @Test
  public void incrementCounterUpdatesRegistry() {
    MeterRegistry registry = new SimpleMeterRegistry();
    MetricService metricService = new MetricService(registry);

    metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);

    assertThat(registry.get(MetricConstants.LISTENER_TABLE_SUCCESS).counter().count(), is(1.0));
  }

  @Test
  public void unknownCounterNameIsIgnoredWithoutException() {
    MetricService metricService = new MetricService(new SimpleMeterRegistry());
    metricService.incrementCounter("not_a_real_counter");
  }

  @Test
  public void allMetricsRegisteredOnConstruction() {
    MeterRegistry registry = new SimpleMeterRegistry();
    new MetricService(registry);

    for (String name : MetricConstants.LISTENER_METRICS) {
      assertThat("expected counter " + name, registry.get(name).counter().count(), is(0.0));
    }
  }

  @Test
  public void recordDurationRegistersTimer() {
    MeterRegistry registry = new SimpleMeterRegistry();
    MetricService metricService = new MetricService(registry);

    metricService.recordDuration(MetricConstants.LISTENER_TABLE_RENAME_DURATION, 500L);

    assertThat(registry.get(MetricConstants.LISTENER_TABLE_RENAME_DURATION).timer().count(), is(1L));
    assertThat(registry.get(MetricConstants.LISTENER_TABLE_RENAME_DURATION).timer().totalTime(TimeUnit.MILLISECONDS), is(500.0));
  }

  @Test
  public void jmxRegistryExposesCountersAsMBeans() throws Exception {
    JmxMeterRegistry jmxRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
    MetricService metricService = new MetricService(jmxRegistry);

    metricService.incrementCounter(MetricConstants.LISTENER_TABLE_SUCCESS);

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beans = mbs.queryNames(new ObjectName("metrics:*"), null);
    boolean found = beans.stream()
        .anyMatch(n -> n.toString().contains(MetricConstants.LISTENER_TABLE_SUCCESS));
    assertThat("expected JMX MBean for " + MetricConstants.LISTENER_TABLE_SUCCESS, found, is(true));

    jmxRegistry.close();
  }

  @Test
  public void taggedEventCounterExposesTagsAsJmxKeyProperties() throws Exception {
    MetricRegistry dropwizardRegistry = new MetricRegistry();
    JmxReporter reporter = JmxReporter.forRegistry(dropwizardRegistry)
        .inDomain("metrics")
        .createsObjectNamesWith(new TaggedObjectNameFactory())
        .build();
    JmxMeterRegistry jmxRegistry = new JmxMeterRegistry(
        JmxConfig.DEFAULT, Clock.SYSTEM,
        MetricService.taggedNameMapper(), dropwizardRegistry, reporter);
    MetricService metricService = new MetricService(jmxRegistry);

    metricService.recordEvent("alter_table", "failure", "other");

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> beans = mbs.queryNames(
        new ObjectName("metrics:name=" + MetricConstants.LISTENER_EVENT + ",*"), null);
    assertThat("expected exactly one event MBean", beans.size(), is(1));
    ObjectName bean = beans.iterator().next();
    assertThat(bean.getKeyProperty("operation"), is("alter_table"));
    assertThat(bean.getKeyProperty("result"), is("failure"));
    assertThat(bean.getKeyProperty("outcome"), is("other"));

    jmxRegistry.close();
  }
}
