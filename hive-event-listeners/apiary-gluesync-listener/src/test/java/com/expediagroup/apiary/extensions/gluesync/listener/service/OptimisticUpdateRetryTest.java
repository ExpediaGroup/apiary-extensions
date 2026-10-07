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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.amazonaws.services.glue.model.ConcurrentModificationException;

public class OptimisticUpdateRetryTest {

  private final AtomicInteger attempts = new AtomicInteger();
  private final AtomicInteger fallbacks = new AtomicInteger();
  private final List<Integer> conflicts = new ArrayList<>();

  @Test
  public void succeedsOnFirstAttempt_noConflictNoFallback() {
    String result = OptimisticUpdateRetry.retryOnConflict(3, conflicts::add,
        () -> {
          attempts.incrementAndGet();
          return "attempt";
        },
        () -> {
          fallbacks.incrementAndGet();
          return "fallback";
        });

    assertThat(result, is("attempt"));
    assertThat(attempts.get(), is(1));
    assertThat(conflicts.isEmpty(), is(true));
    assertThat(fallbacks.get(), is(0));
  }

  @Test
  public void retriesThenSucceeds_recordingEachConflict() {
    String result = OptimisticUpdateRetry.retryOnConflict(3, conflicts::add,
        () -> {
          if (attempts.incrementAndGet() < 3) {
            throw new ConcurrentModificationException("conflict");
          }
          return "attempt";
        },
        () -> {
          fallbacks.incrementAndGet();
          return "fallback";
        });

    assertThat(result, is("attempt"));
    assertThat(attempts.get(), is(3));
    assertThat(conflicts, is(java.util.Arrays.asList(1, 2)));
    assertThat(fallbacks.get(), is(0));
  }

  @Test
  public void runsFallbackOnce_whenEveryAttemptConflicts() {
    String result = OptimisticUpdateRetry.retryOnConflict(3, conflicts::add,
        () -> {
          attempts.incrementAndGet();
          throw new ConcurrentModificationException("conflict");
        },
        () -> {
          fallbacks.incrementAndGet();
          return "fallback";
        });

    assertThat(result, is("fallback"));
    assertThat(attempts.get(), is(3));
    assertThat(conflicts, is(java.util.Arrays.asList(1, 2, 3)));
    assertThat(fallbacks.get(), is(1));
  }

  @Test
  public void propagatesNonConflictException_withoutFallback() {
    try {
      OptimisticUpdateRetry.retryOnConflict(3, conflicts::add,
          () -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("boom");
          },
          () -> {
            fallbacks.incrementAndGet();
            return "fallback";
          });
      fail("expected IllegalStateException to propagate");
    } catch (IllegalStateException expected) {
      // expected
    }

    assertThat(attempts.get(), is(1));
    assertThat(conflicts.isEmpty(), is(true));
    assertThat(fallbacks.get(), is(0));
  }

  @Test
  public void singleAttempt_fallsBackImmediatelyOnConflict() {
    String result = OptimisticUpdateRetry.retryOnConflict(1, conflicts::add,
        () -> {
          attempts.incrementAndGet();
          throw new ConcurrentModificationException("conflict");
        },
        () -> {
          fallbacks.incrementAndGet();
          return "fallback";
        });

    assertThat(result, is("fallback"));
    assertThat(attempts.get(), is(1));
    assertThat(conflicts, is(java.util.Collections.singletonList(1)));
    assertThat(fallbacks.get(), is(1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNonPositiveMaxAttempts() {
    OptimisticUpdateRetry.retryOnConflict(0, conflicts::add, () -> "attempt", () -> "fallback");
  }
}
