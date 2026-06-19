/**
 * Copyright (C) 2018-2026 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.gluesync.listener.metrics;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import javax.management.ObjectName;

import org.junit.Test;

public class TaggedObjectNameFactoryTest {

  private final TaggedObjectNameFactory factory = new TaggedObjectNameFactory();

  @Test
  public void untaggedMetricProducesStandardObjectName() throws Exception {
    ObjectName name = factory.createName("meters", "metrics", "glue_listener_table_failure");

    assertThat(name.getDomain(), is("metrics"));
    assertThat(name.getKeyProperty("name"), is("glue_listener_table_failure"));
    assertThat(name.getKeyProperty("type"), is("meters"));
  }

  @Test
  public void taggedMetricPromotesTagsToKeyProperties() throws Exception {
    ObjectName name = factory.createName("meters", "metrics",
        "glue_listener_event[operation=alter_table,outcome=other,result=failure]");

    assertThat(name.getDomain(), is("metrics"));
    assertThat(name.getKeyProperty("name"), is("glue_listener_event"));
    assertThat(name.getKeyProperty("type"), is("meters"));
    assertThat(name.getKeyProperty("operation"), is("alter_table"));
    assertThat(name.getKeyProperty("outcome"), is("other"));
    assertThat(name.getKeyProperty("result"), is("failure"));
  }

  @Test
  public void untaggedMetricHasNoExtraKeyProperties() throws Exception {
    ObjectName name = factory.createName("meters", "metrics", "glue_listener_table_success");

    assertThat(name.getKeyProperty("operation"), is(nullValue()));
  }
}
