/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

public class MetricService {

  private static final Logger log = LoggerFactory.getLogger(MetricService.class);
  private final Map<String, Counter> metrics;

  public MetricService() {
    this.metrics = MetricConstants.LISTENER_METRICS.stream()
        .collect(Collectors.toMap(
            metricName -> metricName,
            metricName -> Counter.builder(metricName).register(Metrics.globalRegistry)
        ));
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
}
