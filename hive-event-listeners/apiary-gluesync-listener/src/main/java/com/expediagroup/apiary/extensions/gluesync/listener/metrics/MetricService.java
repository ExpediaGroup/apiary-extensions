package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import java.util.Optional;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricService {

  private static final Logger log = LoggerFactory.getLogger(MetricService.class);

  public Optional<Long> incrementCounter(String name) {
    try {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        return Optional.of(metrics.incrementCounter(name));
      }
    } catch (Exception e) {
      log.warn("Unable to increment counter {}", name, e);
    }
    return Optional.empty();
  }
}
