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
package com.expediagroup.apiary.extensions.gluesync.listener;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.handlers.HandlerContextKey;
import com.amazonaws.handlers.RequestHandler2;

import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants;
import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricService;

/**
 * AWS SDK v1 RequestHandler2 that records Glue client metrics via Micrometer.
 *
 * Fires per HTTP attempt (including retries), giving accurate latency and retry amplification visibility:
 *   glue_client_call_duration{operation, result}
 *   glue_client_error_total{operation, error_code}
 */
public class GlueMetricRequestHandler extends RequestHandler2 {

  static final HandlerContextKey<Long> START_TIME = new HandlerContextKey<>("GlueCallStartTime");

  private final MetricService metricService;

  public GlueMetricRequestHandler(MetricService metricService) {
    this.metricService = metricService;
  }

  @Override
  public void beforeRequest(Request<?> request) {
    request.addHandlerContext(START_TIME, System.currentTimeMillis());
  }

  @Override
  public void afterResponse(Request<?> request, Response<?> response) {
    metricService.recordGlueCallDuration(operationName(request), MetricConstants.RESULT_SUCCESS, elapsed(request));
  }

  @Override
  public void afterError(Request<?> request, Response<?> response, Exception e) {
    String operation = operationName(request);
    metricService.recordGlueCallDuration(operation, MetricConstants.RESULT_FAILURE, elapsed(request));
    metricService.recordGlueClientError(operation, errorCode(e));
  }

  private long elapsed(Request<?> request) {
    Long start = request.getHandlerContext(START_TIME);
    return start != null ? System.currentTimeMillis() - start : 0L;
  }

  private String operationName(Request<?> request) {
    AmazonWebServiceRequest original = request.getOriginalRequest();
    if (original == null) {
      return "unknown";
    }
    String name = original.getClass().getSimpleName();
    if (name.endsWith("Request")) {
      name = name.substring(0, name.length() - 7);
    }
    // CamelCase -> snake_case: "GetTable" -> "get_table"
    return name.replaceAll("([A-Z])", "_$1").toLowerCase().replaceFirst("^_", "");
  }

  private String errorCode(Exception e) {
    if (e instanceof AmazonServiceException) {
      String code = ((AmazonServiceException) e).getErrorCode();
      return code != null ? code : "unknown";
    }
    return e.getClass().getSimpleName();
  }
}
