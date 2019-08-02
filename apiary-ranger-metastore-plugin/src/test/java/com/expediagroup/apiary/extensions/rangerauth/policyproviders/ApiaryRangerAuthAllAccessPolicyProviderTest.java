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
package com.expediagroup.apiary.extensions.rangerauth.policyproviders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.expediagroup.apiary.extensions.rangerauth.listener.RangerAdminClientImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.collections.Sets;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Date;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class ApiaryRangerAuthAllAccessPolicyProviderTest {
  @Mock
  private RangerAdminRESTClient rangerAdminRESTClient;
  @Mock
  private Configuration configuration;

  private String serviceName = "apiary";
  private String appId = "appid";
  private String configPropertyPrefix = "prefix";

  @Test
  public void init() {
    ApiaryRangerAuthAllAccessPolicyProvider apiaryRangerAuthAllAccessPolicyProvider =
        new ApiaryRangerAuthAllAccessPolicyProvider(rangerAdminRESTClient);

    apiaryRangerAuthAllAccessPolicyProvider.init(serviceName, appId, configPropertyPrefix);
    verify(rangerAdminRESTClient, times(1)).init(serviceName, appId, configPropertyPrefix);

    ServicePolicies servicePolicies = apiaryRangerAuthAllAccessPolicyProvider.getServicePolicies();
    assertThat(servicePolicies.getPolicies(), hasSize(1));

    assertThat(servicePolicies.getPolicies().get(0).getPolicyItems(), hasSize(1));

    assertThat(servicePolicies.getPolicies().get(0).getPolicyItems().get(0).getGroups(),
        hasItems(ApiaryRangerAuthAllAccessPolicyProvider.ALL_USERS_GROUP));

    assertThat(servicePolicies.getPolicies().get(0).getPolicyItems().get(0).getAccesses(),
        hasSize(ApiaryRangerAuthAllAccessPolicyProvider.ACCESS_NAMES.size()));
  }

  @Test
  public void getServicePoliciesIfUpdated() throws Exception {

    RangerAdminClientImpl testImpl = new RangerAdminClientImpl();
    testImpl.init(serviceName, appId, "ranger.plugin.hive");
    ServicePolicies jsonPolicies = testImpl.getServicePoliciesIfUpdated(1L, 1L);

    ApiaryRangerAuthAllAccessPolicyProvider apiaryRangerAuthAllAccessPolicyProvider =
        new ApiaryRangerAuthAllAccessPolicyProvider(testImpl);

    apiaryRangerAuthAllAccessPolicyProvider.init(serviceName, appId, configPropertyPrefix);

    ServicePolicies servicePolicies = apiaryRangerAuthAllAccessPolicyProvider.
        getServicePoliciesIfUpdated(1L, 1L);

    // validate that we are *not* getting back the policies from the test JSON policy file, and that we *are*
    // getting back the service definition from the test JSON policy file.

    assertThat(jsonPolicies.getPolicies(), hasSize(10));
    assertThat(servicePolicies.getPolicies(), hasSize(1));

    assertThat(jsonPolicies.getServiceDef().getGuid(), equalTo(servicePolicies.getServiceDef().getGuid()));
  }

  @Test
  public void pluginAllowsAllAccess() throws Exception {
    RangerAdminClientImpl testImpl = new RangerAdminClientImpl();
    testImpl.init(serviceName, appId, "ranger.plugin.hive");
    ServicePolicies jsonPolicies = testImpl.getServicePoliciesIfUpdated(1L, 1L);

    ApiaryRangerAuthAllAccessPolicyProvider apiaryRangerAuthAllAccessPolicyProvider =
        new ApiaryRangerAuthAllAccessPolicyProvider(testImpl);

    apiaryRangerAuthAllAccessPolicyProvider.init(serviceName, appId, configPropertyPrefix);

    ServicePolicies servicePolicies = apiaryRangerAuthAllAccessPolicyProvider.
        getServicePoliciesIfUpdated(1L, 1L);

    RangerPolicyEngineImpl rangerPolicyEngine = new RangerPolicyEngineImpl(appId, servicePolicies, null);

    RangerAccessRequest request = getAccessRequest(servicePolicies.getServiceDef(), "testdb", "testtable", "testuser");
    RangerAccessResult result = rangerPolicyEngine.evaluatePolicies(request, RangerPolicy.POLICY_TYPE_ACCESS, null);
    assertThat(result.getIsAllowed(), equalTo(true));
  }

  private RangerAccessRequest getAccessRequest(RangerServiceDef serviceDef, String databaseName, String tableName, String user) {
    RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
    resource.setServiceDef(serviceDef);
    resource.setValue("database", databaseName);
    resource.setValue("table", tableName);

    Set<String> groups = Sets.newSet(user + "_group1", user + "_group2");
    RangerAccessRequestImpl request = new RangerAccessRequestImpl(resource, "create", user, groups);
    request.setAccessTime(new Date());
    request.setAction(HiveOperationType.CREATETABLE.name());

    return request;
  }
}
