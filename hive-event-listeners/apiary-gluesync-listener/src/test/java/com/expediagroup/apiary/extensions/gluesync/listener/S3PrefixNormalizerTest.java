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
package com.expediagroup.apiary.extensions.gluesync.listener;

import org.junit.Test;

import com.expediagroup.apiary.extensions.gluesync.listener.service.S3PrefixNormalizer;

public class S3PrefixNormalizerTest {

  private final S3PrefixNormalizer normalizer = new S3PrefixNormalizer();

  @Test
  public void s3a_location() {
    String s3aLocation = "s3a://my-bucket/path/to/table";
    assert normalizer.normalizeLocation(s3aLocation).equals("s3://my-bucket/path/to/table");
  }

  @Test
  public void s3n_location() {
    String s3aLocation = "s3n://my-bucket/path/to/table";
    assert normalizer.normalizeLocation(s3aLocation).equals("s3://my-bucket/path/to/table");
  }

  @Test
  public void s3x_location() {
    String s3aLocation = "s3x://my-bucket/path/to/table";
    assert normalizer.normalizeLocation(s3aLocation).equals("s3://my-bucket/path/to/table");
  }

  @Test
  public void no_prefix_no_changes() {
    String s3aLocation = "my-bucket/path/to/table";
    assert normalizer.normalizeLocation(s3aLocation).equals("my-bucket/path/to/table");
  }

  @Test
  public void double_slash_after_prefix() {
    String s3aLocation = "s3n://my-bucket//path//to/table";
    assert normalizer.normalizeLocation(s3aLocation).equals("s3://my-bucket//path//to/table");
  }
}
