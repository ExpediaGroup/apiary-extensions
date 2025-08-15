/**
 * Copyright (C) 2018-2025 Expedia, Inc.
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

import java.util.regex.Pattern;

public class S3PrefixNormalizer {

  private static final Pattern S3_PREFIX_PATTERN = Pattern.compile("^s3[a-z]*://");

  public String normalizeLocation(String location) {
    if (location == null) {return null;}
    return S3_PREFIX_PATTERN.matcher(location).replaceFirst("s3://");
  }
}
