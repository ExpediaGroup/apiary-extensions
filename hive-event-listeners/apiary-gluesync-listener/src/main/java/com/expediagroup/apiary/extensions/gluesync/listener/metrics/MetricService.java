package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
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
    incrementCounterInHive(name);
    incrementCounterInMicrometer(name);
  }

  private void incrementCounterInMicrometer(String name) {
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

  private void incrementCounterInHive(String name) {
    try {
      org.apache.hadoop.hive.common.metrics.common.Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        metrics.incrementCounter(name);
      }
    } catch (Exception e) {
      log.warn("Unable to increment counter {}", name, e);
    }
  }
}
