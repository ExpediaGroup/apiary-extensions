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
package com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.lambda;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.serverless.proxy.internal.testutils.MockLambdaConsoleLogger;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.beust.jcommander.internal.Lists;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.exception.HiveClientException;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClient;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClientFactory;
import com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core.PrivilegesGrantor;
import com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core.PriviligesGrantorFactory;

@RunWith(MockitoJUnitRunner.class)
public class PrivilegesGrantorLambdaTest {

  public final @Rule EnvironmentVariables env = new EnvironmentVariables();

  private @Mock PriviligesGrantorFactory priviligesGrantorFactory;
  private @Mock ThriftHiveClientFactory thriftHiveClientFactory;
  private @Mock Context context;
  private @Mock ThriftHiveClient thriftHiveClient;
  private @Mock IMetaStoreClient client;
  private @Mock PrivilegesGrantor privilegesGrantor;

  private PrivilegesGrantorLambda privilegesGrantorLambda;

  @Before
  public void setUp() {
    env.set("THRIFT_CONNECTION_URI", "thrift://uri:9083");
    when(thriftHiveClientFactory.newInstance("thrift://uri:9083", "20000")).thenReturn(thriftHiveClient);
    when(thriftHiveClient.getMetaStoreClient()).thenReturn(client);
    when(priviligesGrantorFactory.newInstance(client)).thenReturn(privilegesGrantor);
    when(context.getLogger()).thenReturn(new MockLambdaConsoleLogger());
    privilegesGrantorLambda = new PrivilegesGrantorLambda(priviligesGrantorFactory, thriftHiveClientFactory);
  }

  @Test
  public void createTable() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record = new SQSEvent.SQSMessage();
    record.setBody(createTableJson());
    event.setRecords(Lists.newArrayList(record));
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    verify(privilegesGrantor).grantSelectPrivileges("some_db", "some_table1");
    assertThat(response.getStatusCode(), is(HttpStatus.SC_OK));
  }

  @Test
  public void renameTable() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record = new SQSEvent.SQSMessage();
    record.setBody(alterTableRenameJson());
    event.setRecords(Lists.newArrayList(record));
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    verify(privilegesGrantor).grantSelectPrivileges("some_db", "some_table2");
    assertThat(response.getStatusCode(), is(HttpStatus.SC_OK));
  }

  @Test
  public void multipleRecords() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record1 = new SQSEvent.SQSMessage();
    record1.setBody(alterTableRenameJson());
    SQSEvent.SQSMessage record2 = new SQSEvent.SQSMessage();
    record2.setBody(createTableJson());
    event.setRecords(Lists.newArrayList(record1, record2));
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    verify(privilegesGrantor).grantSelectPrivileges("some_db", "some_table1");
    verify(privilegesGrantor).grantSelectPrivileges("some_db", "some_table2");
    assertThat(response.getStatusCode(), is(HttpStatus.SC_OK));
  }

  @Test
  public void invalidJson() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record = new SQSEvent.SQSMessage();
    record.setBody("errrrrr");
    event.setRecords(Lists.newArrayList(record));
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    verifyZeroInteractions(privilegesGrantor);
    assertThat(response.getStatusCode(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
  }

  @Test
  public void invalidClient() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record = new SQSEvent.SQSMessage();
    record.setBody(createTableJson());
    event.setRecords(Lists.newArrayList(record));
    when(thriftHiveClientFactory.newInstance("thrift://uri:9083", "20000"))
        .thenThrow(new HiveClientException("init error"));
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    verifyZeroInteractions(privilegesGrantor);
    assertThat(response.getStatusCode(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
  }

  @Test
  public void clientErrorOnFirstRecordsSecondRecordOk() throws Exception {
    SQSEvent event = new SQSEvent();
    SQSEvent.SQSMessage record1 = new SQSEvent.SQSMessage();
    record1.setBody(createTableJson());
    SQSEvent.SQSMessage record2 = new SQSEvent.SQSMessage();
    record2.setBody(alterTableRenameJson());
    event.setRecords(Lists.newArrayList(record1, record2));
    doThrow(new HiveClientException("init error"))
        .doNothing()
        .when(privilegesGrantor)
        .grantSelectPrivileges("some_db", "some_table1");
    Response response = privilegesGrantorLambda.handleRequest(event, context);
    assertThat(response.getStatusCode(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    // should contains both messages
    assertThat(response.getDescription(), containsString("Failed to grant privileges"));
    assertThat(response.getDescription(), containsString("Privileges granted successfully"));
  }

  private String createTableJson() {
    String json = "";
    json += "{\n";
    json += "  \"protocolVersion\": \"1.0\",\n";
    json += "  \"eventType\": \"CREATE_TABLE\",\n";
    json += "  \"dbName\": \"some_db\",\n";
    json += "  \"tableName\": \"some_table1\",\n";
    json += "  \"tableLocation\": \"s3://table_location\"\n";
    json += "}\n";
    return json;
  }

  private String alterTableRenameJson() {
    String json = "";
    json += "{\n";
    json += "  \"protocolVersion\": \"1.0\",\n";
    json += "  \"eventType\": \"ALTER_TABLE\",\n";
    json += "  \"dbName\": \"some_db\",\n";
    json += "  \"tableName\": \"some_table2\",\n";
    json += "  \"tableLocation\": \"s3://table_location\",\n";
    json += "  \"tableParameters\": {\n";
    json += "     \"param_1\": \"val_1\"\n";
    json += "  },\n";
    json += "  \"oldTableName\": \"some_table_old\"\n";
    json += "}\n";
    return json;
  }

}
