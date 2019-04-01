/**
 * Copyright (C) 2018-2019 Expedia Inc.
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
package com.expedia.apiary.extensions.receiver.sqs.model;

import com.expedia.apiary.extensions.receiver.common.messaging.MessageProperty;

public enum SqsMessageProperty implements MessageProperty {

  RECEIPT_HANDLE("receiptHandle");

  private String property;

  SqsMessageProperty(String property) {
    this.property = property;
  }

  public static SqsMessageProperty toSqsMessageProperty(String property) {
    for (SqsMessageProperty p : values()) {
      if (p.getProperty().equals(property)) {
        return p;
      }
    }
    throw new IllegalArgumentException("SqsMessageProperty not found for " + property);
  }

  public String getProperty() {
    return this.property;
  }
}
