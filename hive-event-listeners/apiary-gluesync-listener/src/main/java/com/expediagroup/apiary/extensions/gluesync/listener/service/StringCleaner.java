/**
 * Copyright (C) 2019-2025 Expedia, Inc.
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

public class StringCleaner {

  public String shortTo255Chars(String input) {
    if (input == null) {
      return null;
    }
    if (input.length() > 255) {
      return input.substring(0, 255);
    }
    return input;
  }

  public String clean(String input) {
    if (input == null) {
      return null;
    }
    String result;
    result = removeNonUnicodeChars(input);
    result = removeSpecialChars(result);
    return result;
  }

  private String removeSpecialChars(String comment) {
    return comment.replaceAll("[^a-zA-Z0-9 \\-_,.!?()@#$%^&*+=]", "");
  }

  private String removeNonUnicodeChars(String input) {
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
