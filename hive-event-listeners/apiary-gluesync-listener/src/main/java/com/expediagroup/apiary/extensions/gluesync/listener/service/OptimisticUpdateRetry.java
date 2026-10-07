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
package com.expediagroup.apiary.extensions.gluesync.listener.service;

import java.util.function.IntConsumer;

import com.amazonaws.services.glue.model.ConcurrentModificationException;

/**
 * Retry control flow for AWS Glue optimistic-concurrency writes. Pure logic, with no Glue
 * dependencies of its own, so it can be reasoned about and tested in isolation.
 */
final class OptimisticUpdateRetry {

  private OptimisticUpdateRetry() {}

  /**
   * Runs {@code attempt} up to {@code maxAttempts} times, treating a
   * {@link ConcurrentModificationException} as a retryable optimistic-locking conflict. Returns
   * {@code true} as soon as an attempt completes without conflict. If every attempt conflicts, runs
   * {@code fallback} exactly once and returns {@code false}.
   *
   * <p>Any other exception from {@code attempt} (or from {@code fallback}) propagates to the caller
   * and is not retried. {@code onConflict} is notified with the 1-based attempt number each time an
   * attempt conflicts (e.g. for logging); it is never called for the terminal fallback.
   *
   * @param maxAttempts maximum number of conflict-retryable attempts; must be at least 1
   * @param onConflict  called with the attempt number whenever an attempt conflicts
   * @param attempt     the versioned update to try (and refresh) on each attempt
   * @param fallback    run once if all attempts conflict
   * @return {@code true} if an attempt succeeded, {@code false} if the fallback was used
   */
  static boolean retryOnConflict(int maxAttempts, IntConsumer onConflict, Runnable attempt, Runnable fallback) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be at least 1, was " + maxAttempts);
    }
    for (int attemptNo = 1; attemptNo <= maxAttempts; attemptNo++) {
      try {
        attempt.run();
        return true;
      } catch (ConcurrentModificationException e) {
        onConflict.accept(attemptNo);
      }
    }
    fallback.run();
    return false;
  }
}
