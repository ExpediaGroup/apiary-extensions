package com.expediagroup.apiary.extensions.gluesync.listener.service;

import java.util.regex.Pattern;

public class S3PrefixNormalizer {

  private static final Pattern S3_PREFIX_PATTERN = Pattern.compile("^s3[a-z]*://");

  public String normalizeLocation(String location) {
    if (location == null) {return null;}
    return S3_PREFIX_PATTERN.matcher(location).replaceFirst("s3://");
  }
}
