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
