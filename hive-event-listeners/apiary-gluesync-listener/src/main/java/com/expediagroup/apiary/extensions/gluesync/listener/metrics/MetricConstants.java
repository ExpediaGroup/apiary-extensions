package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import java.util.Arrays;
import java.util.List;

public class MetricConstants {

  public static final String LISTENER_DATABASE_FAILURE = "glue_listener_database_failure";
  public static final String LISTENER_DATABASE_SUCCESS = "glue_listener_database_success";
  public static final String LISTENER_TABLE_FAILURE = "glue_listener_table_failure";
  public static final String LISTENER_TABLE_SUCCESS = "glue_listener_table_success";
  public static final String LISTENER_PARTITION_FAILURE = "glue_listener_partition_failure";
  public static final String LISTENER_PARTITION_SUCCESS = "glue_listener_partition_success";

  public static final List<String> LISTENER_METRICS = Arrays.asList(
      LISTENER_DATABASE_FAILURE,
      LISTENER_DATABASE_SUCCESS,
      LISTENER_TABLE_FAILURE,
      LISTENER_TABLE_SUCCESS,
      LISTENER_PARTITION_FAILURE,
      LISTENER_PARTITION_SUCCESS
  );

  private MetricConstants() {}
}
