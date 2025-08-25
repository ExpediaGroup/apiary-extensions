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


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.expediagroup.apiary.extensions.gluesync.listener.service.IsIcebergTablePredicate;

public class IsIcebergTablePredicateTest {

  private IsIcebergTablePredicate predicate;

  @Before
  public void setUp() {
    predicate = new IsIcebergTablePredicate();
  }

  @Test
  public void testNullTableParameters() {
    assertFalse(predicate.test(null));
  }

  @Test
  public void testEmptyTableParameters() {
    Map<String, String> tableParameters = new HashMap<>();
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  public void testNoMetadataLocationOrTableType() {
    Map<String, String> tableParameters = Collections.singletonMap("some_key", "some_value");
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  public void testHasMetadataLocation() {
    Map<String, String> tableParameters = Collections.singletonMap("metadata_location", "some/location/path");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  public void testHasIcebergTableType() {
    Map<String, String> tableParameters = Collections.singletonMap("table_type", "ICEBERG");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  public void testCaseInsensitiveIcebergType() {
    Map<String, String> tableParameters = Collections.singletonMap("table_type", "IcEbErG");
    assertTrue(predicate.test(tableParameters));
  }

  @Test
  public void testWhitespaceInMetadataLocation() {
    Map<String, String> tableParameters = Collections.singletonMap("metadata_location", "   ");
    assertFalse(predicate.test(tableParameters));
  }

  @Test
  public void testIrrelevantTableType() {
    Map<String, String> tableParameters = Collections.singletonMap("table_type", "hive");
    assertFalse(predicate.test(tableParameters));
  }
}
