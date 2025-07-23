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

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;

import com.expediagroup.apiary.extensions.gluesync.listener.service.GlueMetadataStringCleaner;

public class GlueMetadataStringCleanerTest {

  private GlueMetadataStringCleaner glueMetadataStringCleaner;

  private final List<Column> incorrectColumn = Collections.singletonList(
      new Column().withComment("\uD999" + generateCharString(300))
  );

  @Before
  public void setUp() {
    glueMetadataStringCleaner = new GlueMetadataStringCleaner();
  }

  // Validation from Glue
  //[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDC00-\uDBFF\uDFFF\t]*
  @Test
  public void testTableInput() {
    TableInput tableInput = new TableInput();
    tableInput.setStorageDescriptor(new StorageDescriptor().withColumns(incorrectColumn));
    tableInput.setPartitionKeys(incorrectColumn);
    // verify input is incorrect
    assertTrue(tableInput.getStorageDescriptor().getColumns().get(0).getComment().startsWith("\uD999"));

    TableInput result = glueMetadataStringCleaner.cleanTable(tableInput);

    Column schemaColumn = result.getStorageDescriptor().getColumns().get(0);
    assertTrue(schemaColumn.getComment().length() == 254);
    assertTrue(schemaColumn.getComment().startsWith("A"));

    Column partitionColumn = result.getPartitionKeys().get(0);
    assertTrue(partitionColumn.getComment().length() == 254);
    assertTrue(partitionColumn.getComment().startsWith("A"));
  }

  @Test
  public void testPartitionInput() {
    PartitionInput partitionInput = new PartitionInput();
    partitionInput.setStorageDescriptor(new StorageDescriptor().withColumns(incorrectColumn));
    partitionInput.setValues(Collections.singletonList("\uD999" + generateCharString(300)));
    // verify input is incorrect
    assertTrue(partitionInput.getStorageDescriptor().getColumns().get(0).getComment().startsWith("\uD999"));

    PartitionInput result = glueMetadataStringCleaner.cleanPartition(partitionInput);

    Column schemaColumn = result.getStorageDescriptor().getColumns().get(0);
    assertTrue(schemaColumn.getComment().length() == 254);
    assertTrue(schemaColumn.getComment().startsWith("A")); // non-unicode char removed

    String partitionValue = result.getValues().get(0);
    assertTrue(partitionValue.contentEquals(generateCharString(300)));
    assertTrue(partitionValue.length() == 300);
    assertTrue(partitionValue.startsWith("A")); // non-unicode char removed
  }

  private String generateCharString(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < 300; i++) {
      sb.append("A");
    }
    return sb.toString();
  }
}
