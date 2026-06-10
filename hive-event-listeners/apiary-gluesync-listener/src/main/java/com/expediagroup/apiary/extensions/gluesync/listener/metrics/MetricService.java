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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

public class MetricService {

  private static final Logger log = LoggerFactory.getLogger(MetricService.class);
  private final MeterRegistry registry;
  private final Map<String, Counter> metrics;

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

  private static MeterRegistry configuredRegistry() {
    if (Metrics.globalRegistry.getRegistries().isEmpty()) {
      Metrics.addRegistry(new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM));
    }
    return Metrics.globalRegistry;
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
      Counter.builder(MetricConstants.LISTENER_EVENT)
          .tags(MetricConstants.TAG_OPERATION, operation,
              MetricConstants.TAG_RESULT, result,
              MetricConstants.TAG_OUTCOME, outcome)
          .register(registry)
          .increment();
    } catch (Exception e) {
      log.warn("Unable to record event {} {} {}", operation, result, outcome, e);
    }
  }
}
