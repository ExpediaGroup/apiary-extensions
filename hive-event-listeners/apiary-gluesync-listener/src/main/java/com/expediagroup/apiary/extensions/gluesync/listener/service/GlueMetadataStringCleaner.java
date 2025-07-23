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

import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.TableInput;

/**
 * Following https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-Table
 * validations
 */
public class GlueMetadataStringCleaner {

  public TableInput cleanTable(TableInput input) {
    // Clean SerDes
    cleanColumns(input.getStorageDescriptor().getColumns());
    // Clean Partition Keys
    cleanColumns(input.getPartitionKeys());
    return input;
  }

  public PartitionInput cleanPartition(PartitionInput input) {
    // Clean SerDes
    cleanColumns(input.getStorageDescriptor().getColumns());
    // Clean Partition Keys
    List<String> cleanedKeys = input.getValues().stream().map(this::removeNonUnicodeChars).collect(Collectors.toList());
    input.setValues(cleanedKeys);
    return input;
  }

  private void cleanColumns(List<Column> columns) {
    for (Column column : columns) {
      column.setComment(truncateToMaxAllowedChars(removeNonUnicodeChars(column.getComment())));
    }
  }

  private String truncateToMaxAllowedChars(String input) {
    if (input == null) {
      return null;
    }
    if (input.length() > 254) {
      return input.substring(0, 254);
    }
    return input;
  }

  private String removeNonUnicodeChars(String input) {
    if (input == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < input.length(); i++) {
      int cp = input.codePointAt(i);
      if (isUnicode(cp)) {
        sb.appendCodePoint(cp);
      }
    }
    return sb.toString();
  }

  private boolean isUnicode(int cp) {
    return (
        cp == 0x9 || // tab
            (cp >= 0x20 && cp <= 0xD7FF) ||
            (cp >= 0xE000 && cp <= 0xFFFD) ||
            (cp >= 0x10000 && cp <= 0x10FFFF)
    );
  }
}
