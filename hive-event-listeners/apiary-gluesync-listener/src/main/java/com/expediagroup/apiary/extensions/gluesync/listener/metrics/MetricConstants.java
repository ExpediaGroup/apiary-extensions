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
