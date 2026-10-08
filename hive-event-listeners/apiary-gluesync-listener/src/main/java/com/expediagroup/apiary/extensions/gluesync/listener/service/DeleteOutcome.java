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
 * Outcome of a {@link GlueTableService#deleteIfUnchanged} call, used by callers to record the
 * appropriate metric.
 * <ul>
 *   <li>{@code DELETED} — the Glue table was deleted.</li>
 *   <li>{@code SKIPPED} — the delete was suppressed because identity params diverged, indicating
 *       the Glue slot was overwritten by a concurrent operation.</li>
 *   <li>{@code NOT_FOUND} — the table did not exist in Glue when checked; nothing to delete.</li>
 *   <li>{@code DELETED_CONCURRENTLY} — the table existed at guard-check time but was deleted by a
 *       concurrent operation before the delete call completed (TOCTOU race).</li>
 * </ul>
 */
public enum DeleteOutcome {
  DELETED("deleted"),
  SKIPPED("delete_skipped"),
  NOT_FOUND("not_found"),
  DELETED_CONCURRENTLY("concurrent_delete");

  private final String metricOutcome;

  DeleteOutcome(String metricOutcome) {
    this.metricOutcome = metricOutcome;
  }

  public String metricOutcome() {
    return metricOutcome;
  }
}
