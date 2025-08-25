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
