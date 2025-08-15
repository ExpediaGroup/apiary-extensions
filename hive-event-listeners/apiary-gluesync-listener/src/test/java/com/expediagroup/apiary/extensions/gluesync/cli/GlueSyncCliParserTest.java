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

package com.expediagroup.apiary.extensions.gluesync.cli;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GlueSyncCliParserTest {

  private ByteArrayOutputStream outContent;
  private ByteArrayOutputStream errContent;
  private PrintStream originalOut;
  private PrintStream originalErr;
  private SecurityManager originalSecurityManager;
  static {
    // Use Mockito to mock GlueSyncCli and prevent real constructor logic
    GlueSyncCli mock = mock(GlueSyncCli.class);
    GlueSyncCliParser.setGlueSyncCli(mock);
  }

  @Before
  public void setUp() {
    // Capture System.out and System.err
    outContent = new ByteArrayOutputStream();
    errContent = new ByteArrayOutputStream();
    originalOut = System.out;
    originalErr = System.err;
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));

    // Set up NoExitSecurityManager
    originalSecurityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
  }

  @After
  public void tearDown() {
    // Restore original streams and security manager
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setSecurityManager(originalSecurityManager);
  }

  /**
   * Helper method to execute CLI with args and assert expected exit status
   */
  private void assertCliExitsWithStatus(String[] args, int expectedStatus) {
    try {
      GlueSyncCliParser.main(args);
      fail("Expected SystemExitException");
    } catch (SystemExitException e) {
      assertEquals(expectedStatus, e.status);
    }
  }

  /**
   * Helper method to get the captured output content
   */
  private String getOutputContent() {
    return outContent.toString();
  }

  @Test
  public void testDatabaseAndTableRegexOptionsExitZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*" };
    assertCliExitsWithStatus(args, 0);
    // No output assertions needed; CLI no longer prints regex values
  }

  @Test
  public void testMissingDatabaseOrTableRegexExitsOne()
      throws IOException, MetaException, UnknownDBException, TException {
    String[] args = { "--database-name-regex", "db.*" }; // missing table-name-regex
    assertCliExitsWithStatus(args, 1);

    String output = getOutputContent();
    assertTrue(output.contains("Error"));
    assertTrue(output.contains("usage:"));
    assertTrue(output.contains("GlueSyncCli"));
  }

  @Test
  public void testHelpOptionPrintsUsage() throws IOException, MetaException, UnknownDBException, TException {
    String[] args = { "--help" };
    assertCliExitsWithStatus(args, 0);

    String output = getOutputContent();
    assertTrue(output.contains("usage:"));
    assertTrue(output.contains("GlueSyncCli"));
    assertTrue(output.contains("--help"));
  }

  @Test
  public void testContinueOnErrorOptionExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*", "--continueOnError" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testContinueOnErrorShortOptionExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*", "-c" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testKeepGluePartitionsOptionExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*", "--keep-glue-partitions" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testVerboseOptionExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*", "--verbose" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testVerboseShortOptionExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*", "-v" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testAllOptionsTogetherExitsZero() {
    String[] args = { "--database-name-regex", "db.*", "--table-name-regex", "tbl.*",
        "--verbose", "--continueOnError", "--keep-glue-partitions" };
    assertCliExitsWithStatus(args, 0);
  }

  @Test
  public void testHelpOptionDisplaysAllAvailableOptions() {
    String[] args = { "--help" };
    assertCliExitsWithStatus(args, 0);

    String output = getOutputContent();
    assertTrue("Help should contain database-name-regex option", output.contains("database-name-regex"));
    assertTrue("Help should contain table-name-regex option", output.contains("table-name-regex"));
    assertTrue("Help should contain verbose option", output.contains("verbose"));
    assertTrue("Help should contain help option", output.contains("help"));
    assertTrue("Help should contain continueOnError option", output.contains("continueOnError"));
    assertTrue("Help should contain keep-glue-partitions option", output.contains("keep-glue-partitions"));
  }

  // Helper to catch System.exit for testing
  public static class SystemExitException extends SecurityException {
    public final int status;

    public SystemExitException(int status) {
      this.status = status;
    }
  }

  public static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(java.security.Permission perm) {
    }

    @Override
    public void checkExit(int status) {
      throw new SystemExitException(status);
    }
  }
}
