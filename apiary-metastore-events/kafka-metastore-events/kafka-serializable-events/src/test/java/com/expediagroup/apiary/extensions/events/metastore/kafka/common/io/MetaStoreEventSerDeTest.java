/**
 * Copyright (C) 2018-2019 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.events.metastore.kafka.common.io;

import static org.assertj.core.api.Assertions.assertThat;

import static com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.MetaStoreEventSerDe.serDeForClassName;

import org.junit.Test;

import com.expediagroup.apiary.extensions.events.metastore.kafka.common.KafkaMetaStoreEventsException;
import com.expediagroup.apiary.extensions.events.metastore.kafka.common.io.jackson.JsonMetaStoreEventSerDe;

public class MetaStoreEventSerDeTest {

  @Test
  public void instantiateJsonSerDe() {
    MetaStoreEventSerDe serDe = serDeForClassName(JsonMetaStoreEventSerDe.class.getName());
    assertThat(serDe).isExactlyInstanceOf(JsonMetaStoreEventSerDe.class);
  }

  @Test(expected = KafkaMetaStoreEventsException.class)
  public void invalidSerDeClassName() {
    serDeForClassName("com.expediagroup.apiary.extensions.events.metastore.common.io.unknown.MetaStoreEventSerDe");
  }

}