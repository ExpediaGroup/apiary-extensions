package com.expediagroup.apiary.extensions.events.metastore.metrics;

import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;

public class ExceptionMetrics extends CodahaleMetrics {

  public ExceptionMetrics(HiveConf conf) {
    super(conf);
  }

  @Override
  public Long incrementCounter(String name) {
    throw new RuntimeException();
  }
}
