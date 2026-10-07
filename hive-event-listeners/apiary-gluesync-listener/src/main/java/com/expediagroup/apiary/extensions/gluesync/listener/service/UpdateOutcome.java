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

/**
 * Outcome of a {@link GlueTableService#update} call, used by callers to record the appropriate
 * metric.
 * <ul>
 *   <li>{@code UPDATED} — the Glue table was updated (the normal path).</li>
 *   <li>{@code VERSION_CONFLICT_FALLBACK} — a concurrent writer caused the versioned update to fail
 *       with {@code ConcurrentModificationException}, so the update was retried unconditionally
 *       (without a versionId); the sync still succeeded but that UpdateTable event does not carry a
 *       versionId.</li>
 *   <li>{@code VERSION_UNAVAILABLE_FALLBACK} — the current versionId could not be read, so the
 *       update was done unconditionally (without a versionId); the sync still succeeded but that
 *       UpdateTable event does not carry a versionId.</li>
 * </ul>
 */
public enum UpdateOutcome {
  UPDATED("updated"),
  VERSION_CONFLICT_FALLBACK("version_conflict_fallback"),
  VERSION_UNAVAILABLE_FALLBACK("version_unavailable_fallback");

  private final String metricOutcome;

  UpdateOutcome(String metricOutcome) {
    this.metricOutcome = metricOutcome;
  }

  public String metricOutcome() {
    return metricOutcome;
  }
}
