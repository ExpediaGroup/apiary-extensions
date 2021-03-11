/**
 * Copyright (C) 2018-2021 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.hooks.pathconversion.converters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import com.expediagroup.apiary.extensions.hooks.pathconversion.config.PathConversionConfiguration;
import com.expediagroup.apiary.extensions.hooks.pathconversion.models.PathConversion;

@RunWith(MockitoJUnitRunner.class)
public class PathConverterTest {

  @Mock private PathConversionConfiguration config;
  private PathConverter converter;

  @Before
  public void init() {
    converter = new PathConverter(config);
  }

  @Test
  public void shouldProperlyConvertPath() {
    String testInputLocation = "s3d://some-foo";
    String testOutputLocation = "s3://some-foo";

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("^(s3d)(?:.*)"), "s3", ImmutableList.of(1))
    );

    when(config.getPathConversions()).thenReturn(testConversions);

    StorageDescriptor testSD = sdSetup(testInputLocation);
    boolean result = converter.convertStorageDescriptor(testSD);
    assertTrue(result);
    assertEquals(testOutputLocation, testSD.getLocation());
  }

  @Test
  public void shouldProperlyApplyMultipleConversionsPath() {
    String testInputLocation = "s3d://some-foo";
    String testOutputLocation = "alluxio://some-foo";

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("^(s3d)(?:.*)"), "s3", ImmutableList.of(1)),
        new PathConversion(Pattern.compile("^(s3)(?:.*)"), "alluxio", ImmutableList.of(1))
    );

    when(config.getPathConversions()).thenReturn(testConversions);

    StorageDescriptor testSD = sdSetup(testInputLocation);
    boolean result = converter.convertStorageDescriptor(testSD);
    assertTrue(result);
    assertEquals(testOutputLocation, testSD.getLocation());
  }

  @Test
  public void shouldProperlyApplyMoreComplexRegexExpression() {
    String testInputLocation = "s3://some-foo-us-east-4/some/other/result";
    String alluxioTestPrefix = "alluxio://some-alluxio-url:1234/";
    String testOutputLocation = String.format("%ssome-foo-us-east-4/some/other/result", alluxioTestPrefix);

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("^(s3://)(?:.*us-east-4.*)"), alluxioTestPrefix, ImmutableList.of(1))
    );

    when(config.getPathConversions()).thenReturn(testConversions);

    StorageDescriptor testSD = sdSetup(testInputLocation);
    boolean result = converter.convertStorageDescriptor(testSD);
    assertTrue(result);
    assertEquals(testOutputLocation, testSD.getLocation());
  }

  @Test
  public void shouldProperlyApplyMultipleCaptureGropus() {
    String testInputLocation = "s3://some-foo-us-east-4/some/other-us-east-4/result";
    String testOutputLocation = "s3://some-foo-us-west-4/some/other-us-west-4/result";

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("s3://.*(us-east-4)/.*/.*(us-east-4).*"),
            "us-west-4", ImmutableList.of(1, 2))
    );

    when(config.getPathConversions()).thenReturn(testConversions);

    StorageDescriptor testSD = sdSetup(testInputLocation);
    boolean result = converter.convertStorageDescriptor(testSD);
    assertTrue(result);
    assertEquals(testOutputLocation, testSD.getLocation());
  }

  @Test
  public void shouldProperlyConvertPathForTable() {
    String testInputLocation = "s3d://some-foo";
    String testOutputLocation = "s3://some-foo";

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("^(s3d)(?:.*)"), "s3", ImmutableList.of(1))
    );

    when(config.getPathConversions()).thenReturn(testConversions);
    when(config.isPathConversionEnabled()).thenReturn(true);

    Table srcTable = tableSetup(testInputLocation);

    boolean result = converter.convertTable(srcTable);
    assertTrue(result);
    assertEquals(testOutputLocation, srcTable.getSd().getLocation());
  }

  @Test
  public void shouldDisableConvertPathForTableIfFlagIsDisabled() {
    String testInputLocation = "s3d://some-foo";
    when(config.isPathConversionEnabled()).thenReturn(false);

    Table srcTable = tableSetup(testInputLocation);

    boolean result = converter.convertTable(srcTable);
    assertFalse(result);
    assertEquals(testInputLocation, srcTable.getSd().getLocation());
  }

  @Test
  public void shouldProperlyConvertPathForPartition() {
    String testInputLocation = "s3d://some-foo";
    String testOutputLocation = "s3://some-foo";

    List<PathConversion> testConversions = ImmutableList.of(
        new PathConversion(Pattern.compile("^(s3d)(?:.*)"), "s3", ImmutableList.of(1))
    );

    when(config.getPathConversions()).thenReturn(testConversions);
    when(config.isPathConversionEnabled()).thenReturn(true);

    Partition srcPartition = partitionSetup(testInputLocation);

    boolean result = converter.convertPartition(srcPartition);
    assertTrue(result);
    assertEquals(testOutputLocation, srcPartition.getSd().getLocation());
  }

  @Test
  public void shouldProperlyConvertPathForPartitionIfFlagIsDisabled() {
    String testInputLocation = "s3d://some-foo";
    when(config.isPathConversionEnabled()).thenReturn(false);

    Partition srcPartition = partitionSetup(testInputLocation);

    boolean result = converter.convertPartition(srcPartition);
    assertFalse(result);
    assertEquals(testInputLocation, srcPartition.getSd().getLocation());
  }

  private Table tableSetup(String testInputLocation) {
    StorageDescriptor testSD = sdSetup(testInputLocation);
    Table table = new Table();
    table.setSd(testSD);
    return table;
  }

  private Partition partitionSetup(String testInputLocation) {
    StorageDescriptor testSD = sdSetup(testInputLocation);
    Partition partition = new Partition();
    partition.setSd(testSD);
    return partition;
  }

  private StorageDescriptor sdSetup(String location) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    return sd;
  }
}
