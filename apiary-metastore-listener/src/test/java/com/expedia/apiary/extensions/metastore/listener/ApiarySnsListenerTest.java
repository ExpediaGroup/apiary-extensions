/**
 * Copyright (C) 2018 Expedia Inc.
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
package com.expedia.apiary.extensions.metastore.listener;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.expedia.apiary.extensions.metastore.listener.ApiarySnsListener.PROTOCOL_VERSION;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

@RunWith(MockitoJUnitRunner.class)
public class ApiarySnsListenerTest {

  @Mock
  private AmazonSNSClient snsClient;
  @Mock
  private Configuration configuration;
  @Mock
  private PublishResult publishResult;

  @Captor
  private ArgumentCaptor<PublishRequest> requestCaptor;

  private String tableName = "some_table";
  private String dbName = "some_db";
  private ApiarySnsListener snsListener;

  @Before
  public void setup() {
    snsListener = new ApiarySnsListener(configuration, snsClient);
    when(snsClient.publish(any(PublishRequest.class))).thenReturn(publishResult);
  }

  @Test
  public void onCreateTable() throws MetaException {
    CreateTableEvent event = mock(CreateTableEvent.class);
    when(event.getStatus()).thenReturn(true);
    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(dbName);
    when(event.getTable()).thenReturn(table);

    snsListener.onCreateTable(event);
    verify(snsClient).publish(requestCaptor.capture());
    PublishRequest publishRequest = requestCaptor.getValue();
    assertThat(publishRequest.getMessage(),
        is("{\"protocolVersion\":\"" + PROTOCOL_VERSION
            + "\",\"eventType\":\"CREATE_TABLE\",\"dbName\":\"some_db\",\"tableName\":\"some_table\"}"));
  }

  // TODO: tests for other onXxx() methods
  // TODO: test for setting ARN via environment variable
}
