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

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.expediagroup.apiary.extensions.events.hive.consumer.common.exception.HiveClientException;
import com.expediagroup.apiary.extensions.events.hive.consumer.common.model.HiveMetastoreEvent;
import com.expediagroup.apiary.extensions.events.hive.consumer.common.thrift.ThriftHiveClient;
import com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core.PrivilegesGrantor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpStatus;

/**
 * Consumes events from the SQS queue and grants Public privileges to a table.
 */
public class PrivilegesGrantorLambda implements RequestHandler<SQSEvent, Response> {

  private static final String THRIFT_CONNECTION_URI = System.getenv("THRIFT_CONNECTION_URI");
  private static final String THRIFT_CONNECTION_TIMEOUT = "20000";
  private LambdaLogger Logger;

  @Override
  public Response handleRequest(SQSEvent event, Context context) {
    Logger = context.getLogger();

    List<String> failedEvents = new ArrayList<>();
    List<String> successfulTableNames = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    PrivilegesGrantor privilegesGrantor;

    try {
      privilegesGrantor = new PrivilegesGrantor(new ThriftHiveClient(THRIFT_CONNECTION_URI, THRIFT_CONNECTION_TIMEOUT));
    } catch (HiveClientException e) {
      String responseMessage = String.format("Unable to establish Thrift Connection with the uri %s", THRIFT_CONNECTION_URI);
      Logger.log(responseMessage);
      failedEvents.add(responseMessage);
      return createResponse(successfulTableNames, failedEvents);
    }

    for (SQSEvent.SQSMessage record : event.getRecords()) {
      try {
        Logger.log("Processing Event: " + record.getBody());
        HiveMetastoreEvent hiveMetastoreEvent =
            objectMapper.readValue(record.getBody(), HiveMetastoreEvent.class);
        privilegesGrantor.grantSelectPrivileges(
            hiveMetastoreEvent.getDbName(), hiveMetastoreEvent.getTableName());
        successfulTableNames.add(hiveMetastoreEvent.getTableName());
      } catch (HiveClientException | IOException e) {
        Logger.log("Exception occurred: " + e.toString());
        failedEvents.add(record.getBody());
      }
    }

    return createResponse(successfulTableNames, failedEvents);
  }

  private Response createResponse(List<String> successfulTableNames, List<String> failedEvents) {

    Response response = new Response();
    StringBuffer description = new StringBuffer();

    if (failedEvents.isEmpty()) {
      description.append("Privileges granted successfully: " + successfulTableNames + "\n");
      response.setStatusCode(HttpStatus.SC_OK);
    } else {
      description.append("Failed to grant privileges: " + failedEvents + "\n");
      response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }
    response.setDescription(description.toString());
    return response;
  }
}
