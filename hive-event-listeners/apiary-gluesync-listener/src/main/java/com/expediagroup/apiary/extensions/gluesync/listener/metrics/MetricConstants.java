package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

public class MetricConstants {

  public static final String LISTENER_DATABASE_FAILURE = "glue_listener_database_failure";
  public static final String LISTENER_DATABASE_SUCCESS = "glue_listener_database_success";
  public static final String LISTENER_TABLE_FAILURE = "glue_listener_table_failure";
  public static final String LISTENER_TABLE_SUCCESS = "glue_listener_table_success";
  public static final String LISTENER_PARTITION_FAILURE = "glue_listener_partition_failure";
  public static final String LISTENER_PARTITION_SUCCESS = "glue_listener_partition_success";

  private MetricConstants() {}
}
