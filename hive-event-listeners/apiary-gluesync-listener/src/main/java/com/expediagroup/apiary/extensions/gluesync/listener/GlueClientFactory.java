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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import com.expediagroup.apiary.extensions.gluesync.listener.metrics.MetricService;

/**
 * Builds the AWSGlue client with optional exponential-backoff retries and Micrometer metrics.
 *
 * Retries are disabled by default to avoid blocking HMS threads. Enable for Dronefly (CLI) via:
 *   GLUE_RETRY_ENABLED=true
 *   GLUE_RETRY_MAX_ATTEMPTS=3   (optional, default 3)
 */
public class GlueClientFactory {

  private static final Logger log = LoggerFactory.getLogger(GlueClientFactory.class);

  static final String ENV_RETRY_ENABLED = "GLUE_RETRY_ENABLED";
  static final String ENV_RETRY_MAX_ATTEMPTS = "GLUE_RETRY_MAX_ATTEMPTS";
  static final int DEFAULT_MAX_ATTEMPTS = 3;

  private GlueClientFactory() {}

  public static AWSGlue buildClient(String region, MetricService metricService) {
    return buildClient(region, new ClientConfiguration(), metricService);
  }

  public static AWSGlue buildClient(String region, ClientConfiguration config, MetricService metricService) {
    if (retryEnabled()) {
      int maxAttempts = maxAttempts();
      log.info("Glue client retries enabled: max {} attempts per call", maxAttempts);
      config.setRetryPolicy(buildRetryPolicy(maxAttempts, metricService));
    } else {
      log.info("Glue client retries disabled (set {}=true to enable)", ENV_RETRY_ENABLED);
    }

    AWSGlueClientBuilder builder = AWSGlueClientBuilder.standard()
        .withRegion(region)
        .withClientConfiguration(config);

    if (metricService != null) {
      builder.withRequestHandlers(new GlueMetricRequestHandler(metricService));
    }
    return builder.build();
  }

  static boolean retryEnabled() {
    return "true".equalsIgnoreCase(System.getenv(ENV_RETRY_ENABLED));
  }

  static int maxAttempts() {
    String val = System.getenv(ENV_RETRY_MAX_ATTEMPTS);
    if (val != null) {
      try {
        int n = Integer.parseInt(val.trim());
        if (n > 0) {
          return n;
        }
      } catch (NumberFormatException ignored) {
        log.warn("Invalid value for {}: '{}', using default {}", ENV_RETRY_MAX_ATTEMPTS, val, DEFAULT_MAX_ATTEMPTS);
      }
    }
    return DEFAULT_MAX_ATTEMPTS;
  }

  static RetryPolicy buildRetryPolicy(int maxAttempts, MetricService metricService) {
    RetryPolicy.RetryCondition condition = (request, exception, retriesAttempted) -> {
      if (PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION.shouldRetry(request, exception, retriesAttempted)) {
        recordRetry(metricService, exception.getClass().getSimpleName());
        return true;
      }
      if (exception instanceof AmazonServiceException) {
        String errorCode = ((AmazonServiceException) exception).getErrorCode();
        if ("ConcurrentModificationException".equals(errorCode)) {
          recordRetry(metricService, errorCode);
          return true;
        }
      }
      return false;
    };
    return new RetryPolicy(condition, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, maxAttempts, true);
  }

  private static void recordRetry(MetricService metricService, String exceptionType) {
    if (metricService != null) {
      metricService.recordGlueRetryAttempt(exceptionType);
    }
  }
}
