/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.kafka.common;

public final class Preconditions {

  private Preconditions() {}

  public static String checkNotEmpty(String string, String message) {
    if (string == null || string.trim().isEmpty()) {
      throw new IllegalArgumentException(message);
    }
    return string;
  }

  public static <T> T checkNotNull(T t, String message) {
    if (t == null) {
      throw new NullPointerException(message);
    }
    return t;
  }

}
