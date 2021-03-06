/**
 * Copyright (C) 2018-2021 Expedia, Inc.
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
package com.expediagroup.apiary.extensions.hooks.providers;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;

public class ApiaryNullAuthorizationProvider implements HiveAuthorizationProvider {

  private Configuration conf;
  private HiveAuthenticationProvider hiveAuthenticationProvider;

  /**
   * Return the configuration used by this object.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Set the configuration to be used by this object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init(Configuration conf) throws HiveException {
  }

  @Override
  public HiveAuthenticationProvider getAuthenticator() {
    return hiveAuthenticationProvider;
  }

  @Override
  public void setAuthenticator(HiveAuthenticationProvider authenticator) {
    hiveAuthenticationProvider = authenticator;
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv) throws HiveException, AuthorizationException {
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
  }
}
