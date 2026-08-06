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

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

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
 * Builds the AWSGlue client with optional extra-exception retry policy and Micrometer metrics.
 *
 * Extra retries are disabled by default to avoid blocking HMS threads. Enable for Dronefly (CLI) via:
 *   GLUE_RETRY_ENABLED=true
 *   GLUE_RETRY_MAX_ATTEMPTS=3               (optional, default 3 retries = 4 total calls)
 *   GLUE_RETRY_EXCEPTIONS=ConcurrentModificationException,SomeOtherException   (optional, comma-separated,
 *                                            defaults to ConcurrentModificationException)
 */
public class GlueClientFactory {

  private static final Logger log = LoggerFactory.getLogger(GlueClientFactory.class);

  static final String ENV_RETRY_ENABLED = "GLUE_RETRY_ENABLED";
  static final String ENV_RETRY_MAX_ATTEMPTS = "GLUE_RETRY_MAX_ATTEMPTS";
  static final String ENV_RETRY_EXCEPTIONS = "GLUE_RETRY_EXCEPTIONS";
  static final int DEFAULT_MAX_RETRIES = 3;
  static final String DEFAULT_RETRY_EXCEPTION = "ConcurrentModificationException";

  private GlueClientFactory() {}

  public static AWSGlue buildClient(String region, MetricService metricService) {
    return buildClient(region, new ClientConfiguration(), metricService);
  }

  public static AWSGlue buildClient(String region, ClientConfiguration config, MetricService metricService) {
    if (retryEnabled()) {
      int maxRetries = maxRetries();
      Set<String> retryExceptions = retryExceptions();
      log.info("Glue client custom retry policy active: max {} retries per call (covers throttles, 5xx, and {})",
          maxRetries, retryExceptions);
      config.setRetryPolicy(buildRetryPolicy(maxRetries, retryExceptions, metricService));
    } else {
      log.info("Glue client custom retry policy not active; SDK default retries (throttles/5xx) still apply. Set {}=true to also retry {}.",
          ENV_RETRY_ENABLED, DEFAULT_RETRY_EXCEPTION);
    }

    AWSGlueClientBuilder builder = AWSGlueClientBuilder.standard()
        .withRegion(region)
        .withClientConfiguration(config);

    return builder.build();
  }

  static boolean retryEnabled() {
    return "true".equalsIgnoreCase(System.getenv(ENV_RETRY_ENABLED));
  }

  static int maxRetries() {
    String val = System.getenv(ENV_RETRY_MAX_ATTEMPTS);
    if (val != null) {
      try {
        int n = Integer.parseInt(val.trim());
        if (n > 0) {
          return n;
        }
        log.warn("Invalid value for {}: '{}', using default {}", ENV_RETRY_MAX_ATTEMPTS, val, DEFAULT_MAX_RETRIES);
      } catch (NumberFormatException ignored) {
        log.warn("Invalid value for {}: '{}', using default {}", ENV_RETRY_MAX_ATTEMPTS, val, DEFAULT_MAX_RETRIES);
      }
    }
    return DEFAULT_MAX_RETRIES;
  }

  static Set<String> retryExceptions() {
    String val = System.getenv(ENV_RETRY_EXCEPTIONS);
    if (val == null || val.trim().isEmpty()) {
      return Collections.singleton(DEFAULT_RETRY_EXCEPTION);
    }
    Set<String> exceptions = Arrays.stream(val.split(","))
        .map(String::trim)
        .filter(exceptionType -> !exceptionType.isEmpty())
        .collect(Collectors.toSet());
    return exceptions.isEmpty() ? Collections.singleton(DEFAULT_RETRY_EXCEPTION) : exceptions;
  }

  static RetryPolicy buildRetryPolicy(int maxRetries, MetricService metricService) {
    return buildRetryPolicy(maxRetries, Collections.singleton(DEFAULT_RETRY_EXCEPTION), metricService);
  }

  static RetryPolicy buildRetryPolicy(int maxRetries, Set<String> retryExceptions, MetricService metricService) {
    RetryPolicy.RetryCondition condition = (request, exception, retriesAttempted) -> {
      if (DEFAULT_RETRY_CONDITION.shouldRetry(request, exception, retriesAttempted)) {
        recordRetry(metricService, exceptionTag(exception));
        return true;
      }
      if (exception instanceof AmazonServiceException) {
        String errorCode = ((AmazonServiceException) exception).getErrorCode();
        if (errorCode != null && retryExceptions.contains(errorCode)) {
          recordRetry(metricService, errorCode);
          return true;
        }
      }
      return false;
    };
    return new RetryPolicy(condition, PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY, maxRetries, true);
  }

  private static String exceptionTag(Exception e) {
    if (e instanceof AmazonServiceException) {
      String code = ((AmazonServiceException) e).getErrorCode();
      return code != null ? code : e.getClass().getSimpleName();
    }
    return e.getClass().getSimpleName();
  }

  private static void recordRetry(MetricService metricService, String exceptionType) {
    if (metricService != null) {
      metricService.recordGlueRetryAttempt(exceptionType);
    }
  }
}
