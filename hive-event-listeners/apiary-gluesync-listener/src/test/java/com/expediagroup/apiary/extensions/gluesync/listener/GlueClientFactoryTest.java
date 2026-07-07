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

import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.retry.RetryPolicy;

import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricConstants;
import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricService;

public class GlueClientFactoryTest {

  private SimpleMeterRegistry registry;
  private MetricService metricService;

  @Before
  public void setUp() {
    registry = new SimpleMeterRegistry();
    metricService = new MetricService(registry);
  }

  @Test
  public void defaultMaxAttemptsIsThree() {
    assertThat(GlueClientFactory.maxAttempts(), is(GlueClientFactory.DEFAULT_MAX_ATTEMPTS));
  }

  @Test
  public void retryPolicyRetryCountMatchesMaxAttempts() {
    RetryPolicy policy = GlueClientFactory.buildRetryPolicy(5, null);
    assertThat(policy.getMaxErrorRetry(), is(5));
  }

  @Test
  public void concurrentModificationExceptionIsRetried() {
    RetryPolicy policy = GlueClientFactory.buildRetryPolicy(3, null);
    AmazonServiceException ex = new AmazonServiceException("conflict");
    ex.setErrorCode("ConcurrentModificationException");

    boolean shouldRetry = policy.getRetryCondition().shouldRetry(null, ex, 0);
    assertThat(shouldRetry, is(true));
  }

  @Test
  public void unknownServiceExceptionIsNotRetried() {
    RetryPolicy policy = GlueClientFactory.buildRetryPolicy(3, null);
    AmazonServiceException ex = new AmazonServiceException("bad input");
    ex.setErrorCode("InvalidInputException");
    ex.setStatusCode(400);

    boolean shouldRetry = policy.getRetryCondition().shouldRetry(null, ex, 0);
    assertThat(shouldRetry, is(false));
  }

  @Test
  public void retryRecordsMetricWhenMetricServiceIsPresent() {
    RetryPolicy policy = GlueClientFactory.buildRetryPolicy(3, metricService);
    AmazonServiceException ex = new AmazonServiceException("conflict");
    ex.setErrorCode("ConcurrentModificationException");

    policy.getRetryCondition().shouldRetry(null, ex, 0);

    assertThat(registry.get(MetricConstants.GLUE_RETRY_ATTEMPT)
        .tags(MetricConstants.TAG_EXCEPTION, "ConcurrentModificationException")
        .counter().count(), is(1.0));
  }

  @Test
  public void retryDoesNotThrowWhenMetricServiceIsNull() {
    RetryPolicy policy = GlueClientFactory.buildRetryPolicy(3, null);
    AmazonServiceException ex = new AmazonServiceException("conflict");
    ex.setErrorCode("ConcurrentModificationException");

    // should not throw
    policy.getRetryCondition().shouldRetry(null, ex, 0);
  }

  @Test
  public void retryIsDisabledByDefault() {
    assertThat(GlueClientFactory.retryEnabled(), is(false));
  }
}
