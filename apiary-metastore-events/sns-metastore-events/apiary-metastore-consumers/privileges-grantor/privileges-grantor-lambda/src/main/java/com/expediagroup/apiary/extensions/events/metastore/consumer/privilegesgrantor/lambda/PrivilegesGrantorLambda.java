/**
 * Copyright (C) 2018-2020 Expedia, Inc.
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

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.http.HttpStatus;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.exception.HiveClientException;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClient;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClientFactory;
import com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core.PrivilegesGrantor;
import com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core.PriviligesGrantorFactory;
import com.expediagroup.apiary.extensions.events.receiver.common.error.SerDeException;
import com.expediagroup.apiary.extensions.events.receiver.common.event.AlterTableEvent;
import com.expediagroup.apiary.extensions.events.receiver.common.event.EventType;
import com.expediagroup.apiary.extensions.events.receiver.common.event.ListenerEvent;
import com.expediagroup.apiary.extensions.events.receiver.common.messaging.JsonMetaStoreEventDeserializer;
import com.expediagroup.apiary.extensions.events.receiver.common.messaging.MessageDeserializer;
import com.expediagroup.apiary.extensions.events.receiver.sqs.messaging.DefaultSqsMessageDeserializer;

/**
 * Consumes events from the SQS queue and grants Public privileges to a table.
 */
public class PrivilegesGrantorLambda implements RequestHandler<SQSEvent, Response> {

  private static final String THRIFT_CONNECTION_URI = System.getenv("THRIFT_CONNECTION_URI");
  private static final String THRIFT_CONNECTION_TIMEOUT = "20000";
  private LambdaLogger logger;
  private final PriviligesGrantorFactory priviligesGrantorFactory;
  private final ThriftHiveClientFactory thriftHiveClientFactory;

  public PrivilegesGrantorLambda() {
    this(new PriviligesGrantorFactory(), new ThriftHiveClientFactory());
  }

  @VisibleForTesting
  PrivilegesGrantorLambda(
      PriviligesGrantorFactory priviligesGrantorFactory,
      ThriftHiveClientFactory thriftHiveClientFactory) {
    this.priviligesGrantorFactory = priviligesGrantorFactory;
    this.thriftHiveClientFactory = thriftHiveClientFactory;
  }

  @Override
  public Response handleRequest(SQSEvent event, Context context) {
    logger = context.getLogger();
    List<String> failedEvents = new ArrayList<>();
    List<String> successfulTableNames = new ArrayList<>();
    IMetaStoreClient metaStoreClient = null;
    try {
      ThriftHiveClient thriftHiveClient = thriftHiveClientFactory
          .newInstance(THRIFT_CONNECTION_URI, THRIFT_CONNECTION_TIMEOUT);
      metaStoreClient = thriftHiveClient.getMetaStoreClient();
      PrivilegesGrantor privilegesGrantor = priviligesGrantorFactory.newInstance(metaStoreClient);
      MessageDeserializer metaStoreEventDeserializer = defaultMessageDeserializer();
      for (SQSEvent.SQSMessage record : event.getRecords()) {
        handleRecord(failedEvents, successfulTableNames, privilegesGrantor, metaStoreEventDeserializer, record);
      }
    } catch (HiveClientException e) {
      String responseMessage = String
          .format("Unable to establish Thrift Connection with the uri %s", THRIFT_CONNECTION_URI);
      logger.log(responseMessage);
      failedEvents.add(responseMessage);
      return createResponse(successfulTableNames, failedEvents);
    } finally {
      if (metaStoreClient != null) {
        metaStoreClient.close();
      }
    }
    return createResponse(successfulTableNames, failedEvents);
  }

  private void handleRecord(
      List<String> failedEvents,
      List<String> successfulTableNames,
      PrivilegesGrantor privilegesGrantor,
      MessageDeserializer metaStoreEventDeserializer,
      SQSEvent.SQSMessage record) {
    try {
      logger.log("Processing Event: " + record.getBody());
      ListenerEvent listenerEvent = metaStoreEventDeserializer.unmarshal(record.getBody());
      if (listenerEvent.getEventType() == EventType.CREATE_TABLE) {
        privilegesGrantor.grantSelectPrivileges(listenerEvent.getDbName(), listenerEvent.getTableName());
        successfulTableNames.add(listenerEvent.getTableName());
      }
      if (listenerEvent.getEventType() == EventType.ALTER_TABLE) {
        AlterTableEvent alterTableEvent = (AlterTableEvent) listenerEvent;
        if (!alterTableEvent.getTableName().equals(alterTableEvent.getOldTableName())) {
          privilegesGrantor.grantSelectPrivileges(listenerEvent.getDbName(), listenerEvent.getTableName());
          successfulTableNames.add(listenerEvent.getTableName());
        }
      }
    } catch (HiveClientException | SerDeException e) {
      logger.log("Exception occurred: " + e.toString());
      failedEvents.add(record.getBody());
    }
  }

  private MessageDeserializer defaultMessageDeserializer() {
    ObjectMapper mapper = new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    JsonMetaStoreEventDeserializer delegateSerDe = new JsonMetaStoreEventDeserializer(mapper);
    return new DefaultSqsMessageDeserializer(delegateSerDe, mapper);
  }

  private Response createResponse(List<String> successfulTableNames, List<String> failedEvents) {
    Response response = new Response();
    StringBuffer description = new StringBuffer();
    if (failedEvents.isEmpty()) {
      description.append("Privileges granted successfully: " + successfulTableNames + "\n");
      response.setStatusCode(HttpStatus.SC_OK);
    } else {
      if (!successfulTableNames.isEmpty()) {
        description.append("Privileges granted successfully: " + successfulTableNames + "\n");
      }
      description.append("Failed to grant privileges: " + failedEvents + "\n");
      response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }
    response.setDescription(description.toString());
    return response;
  }
}
