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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.DefaultRequest;
import com.amazonaws.Request;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants;
import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricService;

public class GlueMetricRequestHandlerTest {

  private SimpleMeterRegistry registry;
  private MetricService metricService;
  private GlueMetricRequestHandler handler;

  @Before
  public void setUp() {
    registry = new SimpleMeterRegistry();
    metricService = new MetricService(registry);
    handler = new GlueMetricRequestHandler(metricService);
  }

  @Test
  public void successRecordsDurationWithSuccessTag() {
    Request<GetTableRequest> request = buildRequest(new GetTableRequest());
    handler.beforeRequest(request);
    handler.afterResponse(request, null);

    assertThat(registry.get(MetricConstants.GLUE_CLIENT_CALL_DURATION)
        .tags(MetricConstants.TAG_OPERATION, "get_table", MetricConstants.TAG_RESULT, MetricConstants.RESULT_SUCCESS)
        .timer().count(), is(1L));
  }

  @Test
  public void errorRecordsDurationWithFailureTagAndErrorCounter() {
    Request<UpdateTableRequest> request = buildRequest(new UpdateTableRequest());
    AmazonServiceException exception = new AmazonServiceException("conflict");
    exception.setErrorCode("ConcurrentModificationException");

    handler.beforeRequest(request);
    handler.afterError(request, null, exception);

    assertThat(registry.get(MetricConstants.GLUE_CLIENT_CALL_DURATION)
        .tags(MetricConstants.TAG_OPERATION, "update_table", MetricConstants.TAG_RESULT, MetricConstants.RESULT_FAILURE)
        .timer().count(), is(1L));

    assertThat(registry.get(MetricConstants.GLUE_CLIENT_ERROR_TOTAL)
        .tags(MetricConstants.TAG_OPERATION, "update_table",
            MetricConstants.TAG_ERROR_CODE, "ConcurrentModificationException")
        .counter().count(), is(1.0));
  }

  @Test
  public void nonServiceExceptionUsesSimpleClassName() {
    Request<GetTableRequest> request = buildRequest(new GetTableRequest());
    handler.beforeRequest(request);
    handler.afterError(request, null, new RuntimeException("oops"));

    assertThat(registry.get(MetricConstants.GLUE_CLIENT_ERROR_TOTAL)
        .tags(MetricConstants.TAG_OPERATION, "get_table",
            MetricConstants.TAG_ERROR_CODE, "RuntimeException")
        .counter().count(), is(1.0));
  }

  @Test
  public void missingStartTimeRecordsDurationOfZero() {
    Request<GetTableRequest> request = buildRequest(new GetTableRequest());
    // do NOT call beforeRequest — START_TIME not set
    handler.afterResponse(request, null);

    assertThat(registry.get(MetricConstants.GLUE_CLIENT_CALL_DURATION)
        .tags(MetricConstants.TAG_OPERATION, "get_table", MetricConstants.TAG_RESULT, MetricConstants.RESULT_SUCCESS)
        .timer().totalTime(TimeUnit.MILLISECONDS), is(0.0));
  }

  private <T extends com.amazonaws.AmazonWebServiceRequest> Request<T> buildRequest(T originalRequest) {
    DefaultRequest<T> request = new DefaultRequest<>(originalRequest, "AWSGlue");
    return request;
  }
}
