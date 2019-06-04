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
package com.expediagroup.apiary.extensions.events.metastore.consumer.privilegesgrantor.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static junit.framework.TestCase.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.expediagroup.apiary.extensions.events.metastore.consumer.common.exception.HiveClientException;
import com.expediagroup.apiary.extensions.events.metastore.consumer.common.thrift.ThriftHiveClient;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class PrivilegesGrantorTest {

  private PrivilegesGrantor privilegesGrantor;
  private static final String DB_NAME = "foo_db";
  private static final String TABLE_NAME = "bar_table";

  @Rule
  public ThriftHiveMetaStoreJUnitRule hive = new ThriftHiveMetaStoreJUnitRule(DB_NAME);

  @Before
  public void setUp() throws Exception {
    String connectionURL = hive.getThriftConnectionUri();
    privilegesGrantor = new PrivilegesGrantor(new ThriftHiveClient(connectionURL).getMetaStoreClient());
    createPartitionedTable(DB_NAME, TABLE_NAME);
  }

  @Test
  public void testGrantSelectPrivilegesToPublic() throws TException {

    List<HiveObjectPrivilege> hiveObjectPrivilegeList = hive
        .client()
        .list_privileges(PrincipalName.PUBLIC.toString(), PrincipalType.ROLE, getHiveObjectRef());

    assertEquals(0, hiveObjectPrivilegeList.size());

    privilegesGrantor.grantSelectPrivileges(DB_NAME, TABLE_NAME);

    hiveObjectPrivilegeList = hive
        .client()
        .list_privileges(PrincipalName.PUBLIC.toString(), PrincipalType.ROLE, getHiveObjectRef());
    assertEquals(1, hiveObjectPrivilegeList.size());
    HiveObjectPrivilege hiveObjectPrivilege = hiveObjectPrivilegeList.get(0);
    assertEquals(Privilege.SELECT.toString(), hiveObjectPrivilege.getGrantInfo().getPrivilege());
    assertEquals(Grantor.APIARY_PRIVILEGE_GRANTOR.toString(), hiveObjectPrivilege.getGrantInfo().getGrantor());
    assertEquals(false, hiveObjectPrivilege.getGrantInfo().isGrantOption());
    assertEquals(PrincipalType.ROLE, hiveObjectPrivilege.getGrantInfo().getGrantorType());
    assertEquals(PrincipalType.ROLE, hiveObjectPrivilege.getPrincipalType());
  }

  @Test
  public void testPrivilegeNotGranted() throws Exception {
    createPartitionedTable(DB_NAME, TABLE_NAME);
    assertFalse(privilegesGrantor.isSelectPrivilegeGranted(DB_NAME, DB_NAME));
  }

  @Test
  public void testPrivilegeGranted() {
    privilegesGrantor.grantSelectPrivileges(DB_NAME, TABLE_NAME);
    assertTrue(privilegesGrantor.isSelectPrivilegeGranted(DB_NAME, TABLE_NAME));
  }

  @Test(expected = HiveClientException.class)
  public void testAddingPrivilegesFailure() {
    privilegesGrantor.grantSelectPrivileges(DB_NAME, null);
  }

  private HiveObjectRef getHiveObjectRef() {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_NAME);
    hiveObjectRef.setObjectType(HiveObjectType.TABLE);
    hiveObjectRef.setObjectName(TABLE_NAME);
    return hiveObjectRef;
  }

  private Table createPartitionedTable(String databaseName, String tableName) throws Exception {

    if (hive.client().tableExists(databaseName, tableName)) {
      hive.client().dropTable(databaseName, tableName);
    }

    Table table = new Table();
    table.setDbName(databaseName);
    table.setTableName(tableName);
    table.setSd(new StorageDescriptor());
    table.getSd().setCols(Arrays.asList(new FieldSchema("value", "string", null)));
    table.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    table.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    table.getSd().setSerdeInfo(new SerDeInfo());
    table.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");

    hive.client().createTable(table);

    return table;
  }
}
