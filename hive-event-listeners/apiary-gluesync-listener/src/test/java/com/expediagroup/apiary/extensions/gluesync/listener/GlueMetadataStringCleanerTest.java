package com.expediagroup.apiary.extensions.gluesync.listener;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueMetadataStringCleaner;

public class GlueMetadataStringCleanerTest {

  private GlueMetadataStringCleaner glueMetadataStringCleaner;

  @Before
  public void setUp() {
    glueMetadataStringCleaner = new GlueMetadataStringCleaner();
  }

  @Test
  public void testShortTo254Chars() {
    int length = 300;
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append("A");
    }
    assertTrue(glueMetadataStringCleaner.shortTo254Chars(sb.toString()).length() == 254);
  }

  //[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF\t]*
  @Test
  public void testCleanNonUnicode() {
    String input = "Hello, World!\uD999";
    String expected = "Hello, World!";
    String result = glueMetadataStringCleaner.clean(input);
    System.out.println(input);
    System.out.println(result);
    System.out.println(input.contentEquals(result));
    assertTrue(result.equals(expected));
  }
}
