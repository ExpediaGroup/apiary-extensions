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

import static com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants.LISTENER_EVENT;
import static com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants.TAG_OPERATION;
import static com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants.TAG_OUTCOME;
import static com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants.TAG_RESULT;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

public class MetricService {

  private static final Logger log = LoggerFactory.getLogger(MetricService.class);
  private final MeterRegistry registry;
  private final Map<String, Counter> metrics;
  private final Map<String, Counter> events = new ConcurrentHashMap<>();

  public MetricService(MeterRegistry registry) {
    this.registry = registry;
    this.metrics = MetricConstants.LISTENER_METRICS.stream()
        .collect(Collectors.toMap(
            metricName -> metricName,
            metricName -> Counter.builder(metricName).register(registry)
        ));
  }

  public MetricService() {
    this(configuredRegistry());
  }

  // DO NOT extract to a shared utility. KafkaMessageReaderBuilder in kafka-metastore-receiver
  // contains an identical copy, but this module shades and relocates micrometer-jmx because it
  // runs inside HMS (classpath conflicts). Shading breaks Spring Boot auto-configuration,
  // so each module must own this method and use the correct (shaded or unshaded) JmxMeterRegistry
  // for its deployment context. Keep these two copies in sync manually.
  private static synchronized MeterRegistry configuredRegistry() {
    if (Metrics.globalRegistry.getRegistries().isEmpty()) {
      MetricRegistry dropwizardRegistry = new MetricRegistry();
      JmxReporter reporter = JmxReporter.forRegistry(dropwizardRegistry)
          .inDomain("metrics")
          .createsObjectNamesWith(new TaggedObjectNameFactory())
          .build();
      Metrics.addRegistry(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM, taggedNameMapper(), dropwizardRegistry, reporter));
    }
    return Metrics.globalRegistry;
  }

  // Encodes Micrometer tags as "name[key=value,key=value]" in the Dropwizard metric name so that
  // TaggedObjectNameFactory can promote them to proper JMX ObjectName key properties.
  static HierarchicalNameMapper taggedNameMapper() {
    return (id, convention) -> {
      String baseName = convention.name(id.getName(), id.getType(), id.getBaseUnit());
      List<Tag> tags = id.getTags();
      if (tags.isEmpty()) {
        return baseName;
      }
      StringBuilder sb = new StringBuilder(baseName).append('[');
      for (int i = 0; i < tags.size(); i++) {
        Tag tag = tags.get(i);
        if (i > 0) {
          sb.append(',');
        }
        sb.append(convention.tagKey(tag.getKey()))
            .append('=')
            .append(convention.tagValue(tag.getValue()));
      }
      return sb.append(']').toString();
    };
  }

  public void incrementCounter(String name) {
    try {
      Counter counter = metrics.get(name);
      if (counter != null) {
        counter.increment();
      } else {
        log.warn("Counter {} not found in Micrometer registry", name);
      }
    } catch (Exception e) {
      log.warn("Unable to increment counter {}", name, e);
    }
  }

  public void recordDuration(String name, long durationMs) {
    try {
      Timer.builder(name)
          .register(registry)
          .record(durationMs, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.warn("Unable to record duration {} {}ms", name, durationMs, e);
    }
  }

  public void recordEvent(String operation, String result, String outcome) {
    try {
      events.computeIfAbsent(operation + "|" + result + "|" + outcome, k ->
          Counter.builder(LISTENER_EVENT)
              .tags(TAG_OPERATION, operation, TAG_RESULT, result, TAG_OUTCOME, outcome)
              .register(registry))
          .increment();
    } catch (Exception e) {
      log.warn("Unable to record event {} {} {}", operation, result, outcome, e);
    }
  }
}
