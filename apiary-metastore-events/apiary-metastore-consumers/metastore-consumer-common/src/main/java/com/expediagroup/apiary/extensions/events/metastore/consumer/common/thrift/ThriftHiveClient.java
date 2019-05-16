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
package com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.exception.HiveClientException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

/**
 * Exposes an instance of the Hive Metastore Thrift Api Client.
 */
public class ThriftHiveClient {

  private IMetaStoreClient client;
  private static final String DEFAULT_TIMEOUT = "5000";

  public ThriftHiveClient(String hiveMetaStoreUris) {
    this(hiveMetaStoreUris, DEFAULT_TIMEOUT);
  }

  public ThriftHiveClient(String hiveMetaStoreUris, String hiveMetastoreClientSocketTimeout) {
    try {
      HiveConf hiveConf = new HiveConf();
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreUris);

      if (hiveMetastoreClientSocketTimeout != null) {
        hiveConf.setVar(
            HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, hiveMetastoreClientSocketTimeout);
      }
      hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveMetaStoreUris);

      this.client = new HiveMetaStoreClient(hiveConf);
    } catch (Exception e) {
      throw new HiveClientException("Error creating hive metastore client", e);
    }
  }

  public IMetaStoreClient getMetaStoreClient() {
    return this.client;
  }
}
