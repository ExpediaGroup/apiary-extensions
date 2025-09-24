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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.help.HelpFormatter;
import org.apache.thrift.TException;

public class GlueSyncCliParser {

  private static GlueSyncCli glueSyncCli;

  /**
   * For testing purposes only
   */
  public static void setGlueSyncCli(GlueSyncCli glueSyncCli) {
    GlueSyncCliParser.glueSyncCli = glueSyncCli;
  }

  public static void main(String[] args) throws TException {
    org.apache.log4j.Logger root = org.apache.log4j.Logger.getRootLogger();

    org.apache.log4j.Logger apiaryLogger = org.apache.log4j.Logger
        .getLogger("com.expediagroup.apiary.extensions.gluesync");

    if (!root.getAllAppenders().hasMoreElements()) {
      root.removeAllAppenders();
      root.setLevel(org.apache.log4j.Level.OFF);

      org.apache.log4j.ConsoleAppender console = new org.apache.log4j.ConsoleAppender();
      console.setName("ApiaryConsole");
      console.setLayout(new org.apache.log4j.PatternLayout("%d{ISO8601} %-5p [%t] %c - %m%n"));
      console.setTarget("System.out");
      console.activateOptions();

      apiaryLogger.addAppender(console);
      apiaryLogger.setAdditivity(false);
    }

    Options options = new Options();
    CommandLineParser parser = getParser(options);

    try {
      CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("verbose")) {
        apiaryLogger.setLevel(org.apache.log4j.Level.DEBUG);
      } else {
        apiaryLogger.setLevel(org.apache.log4j.Level.INFO);
      }

      if (cmd.hasOption("help")) {
        printUsage(options);
        System.exit(0);
      }

      if (glueSyncCli == null) {
        glueSyncCli = new GlueSyncCli();
      }
      glueSyncCli.syncAll(cmd);

      System.exit(0);
    } catch (ParseException e) {
      for (String arg : args) {
        if ("--help".equals(arg)) {
          printUsage(options);
          System.exit(0);
        }
      }

      System.out.println("Error parsing command line: " + e.getMessage());
      printUsage(options);
      System.exit(1);
    }
  }

  private static CommandLineParser getParser(Options options) {
    Option dbRegexOpt = new Option(null, "database-name-regex", true, "Regex for database name");
    dbRegexOpt.setRequired(true);

    Option tableRegexOpt = new Option(null, "table-name-regex", true, "Regex for table name");
    tableRegexOpt.setRequired(true);

    options.addOption(dbRegexOpt);
    options.addOption(tableRegexOpt);
    options.addOption(new Option("v", "verbose", false, "Enable verbose output"));
    options.addOption(new Option("h", "help", false, "Print usage information"));
    options.addOption(new Option("c", "continueOnError", false, "Continue on error (default: false)"));
    options.addOption(new Option(null, "keep-glue-partitions", false,
        "If true, will keep glue partitions even if there is no corresponding hive partition. If false will delete them (default: false)"));

    CommandLineParser parser = new DefaultParser();
    return parser;
  }

  private static void printUsage(Options options) {
    String header = "GlueSync CLI - Sync Hive tables to Glue";
    HelpFormatter helpFormatter = HelpFormatter.builder()
        .setShowSince(false)
        .get();
    try {
      helpFormatter.printHelp("GlueSyncCli", header, options, null, true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
