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

import com.expediagroup.apiary.extensions.rangerauth.listener.ApiaryRangerAuthPreEventListener;
import com.google.common.annotations.VisibleForTesting;
import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.admin.client.RangerAdminRESTClient;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.GrantRevokeRequest;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.apache.ranger.plugin.util.ServiceTags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Class to return a list of Ranger authorization policies that permits all access.  If this is configured in
 * ranger-hive-security.xml in the "ranger.plugin.hive.policy.source.impl" property, Ranger will allow all actions.
 * <p>
 * The intent of this class is to only configure it on the Apiary read-only metastore so that Ranger auditing can still
 * be used without Ranger authorization policies.  We have to do it this way, because Ranger auditing is down in the
 * guts of the policy evaluation code, so there isn't a cleaner way to audit without policies that I can find.
 */

public class ApiaryRangerAuthAllAccessPolicyProvider implements RangerAdminClient {
  @VisibleForTesting
  static final List<String> ACCESS_NAMES =
      Arrays.asList("select", "update", "create", "drop", "alter", "index", "lock", "all", "read", "write");

  @VisibleForTesting
  static final List<String> POLICY_RESOURCES = Arrays.asList("database", "column", "table");

  @VisibleForTesting
  static final String ALL_USERS_GROUP = "public";

  private static final Logger log = LoggerFactory.getLogger(ApiaryRangerAuthPreEventListener.class);

  private ServicePolicies servicePolicies;
  private RangerAdminRESTClient rangerAdminRESTClient;


  public ApiaryRangerAuthAllAccessPolicyProvider() {
    log.info("Creating instance of ApiaryRangerAuthAllAccessPolicyProvider for Apiary read-only metastore");
    this.rangerAdminRESTClient = new RangerAdminRESTClient();
  }

  public ApiaryRangerAuthAllAccessPolicyProvider(RangerAdminRESTClient rangerAdminRESTClient) {
    log.info("Creating instance of ApiaryRangerAuthAllAccessPolicyProvider for Apiary read-only metastore");
    this.rangerAdminRESTClient = rangerAdminRESTClient;
  }

  @Override
  public void init(String serviceName, String appId, String configPropertyPrefix) {
    servicePolicies = new ServicePolicies();
    servicePolicies.setServiceName(serviceName);
    servicePolicies.setServiceId(1L);
    servicePolicies.setPolicyVersion(1L);
    servicePolicies.setPolicyUpdateTime(new Date());

    List<RangerPolicy> policies = new ArrayList<>();
    policies.add(getPolicy(1L, serviceName, POLICY_RESOURCES));
    servicePolicies.setPolicies(policies);

    rangerAdminRESTClient.init(serviceName, appId, configPropertyPrefix);
    log.info("Successfully initialized ApiaryRangerAuthAllAccessPolicyProvider");
  }

  @Override
  public ServicePolicies getServicePoliciesIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
    ServicePolicies realServicePolicies = rangerAdminRESTClient.getServicePoliciesIfUpdated(lastKnownVersion, lastActivationTimeInMillis);

    // Use the real Service Definition from Ranger Admin in our hardcoded policies so our policies match the real service def.
    servicePolicies.setServiceDef(realServicePolicies.getServiceDef());
    return servicePolicies;
  }

  @Override
  public void grantAccess(GrantRevokeRequest request) throws Exception {
  }

  @Override
  public void revokeAccess(GrantRevokeRequest request) throws Exception {
  }

  @Override
  public ServiceTags getServiceTagsIfUpdated(long lastKnownVersion, long lastActivationTimeInMillis) throws Exception {
    return null;
  }

  @Override
  public List<String> getTagTypes(String tagTypePattern) throws Exception {
    return null;
  }

  private RangerPolicy getPolicy(long policyId, String serviceName, List<String> resources) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(policyId);
    policy.setService(serviceName);
    policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
    policy.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);

    Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();
    for (String resource : resources) {
      policyResources.put(resource, new RangerPolicy.RangerPolicyResource("*", false, false));
    }
    policy.setResources(policyResources);
    policy.setPolicyItems(getAllPolicyItems());
    policy.setDenyPolicyItems(Collections.emptyList());
    policy.setAllowExceptions(Collections.emptyList());
    policy.setDenyExceptions(Collections.emptyList());
    policy.setValiditySchedules(Collections.emptyList());
    policy.setPolicyLabels(Collections.emptyList());
    policy.setOptions(Collections.emptyMap());

    return policy;
  }

  private List<RangerPolicy.RangerPolicyItem> getAllPolicyItems() {
    RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

    List<RangerPolicy.RangerPolicyItemAccess> accesses = new ArrayList<>();

    for (String accessName : ACCESS_NAMES) {
      accesses.add(new RangerPolicy.RangerPolicyItemAccess(accessName, true));
    }
    policyItem.setAccesses(accesses);
    policyItem.setGroups(Arrays.asList(ALL_USERS_GROUP));
    policyItem.setUsers(Collections.emptyList());
    policyItem.setConditions(Collections.emptyList());
    policyItem.setDelegateAdmin(false);

    return Arrays.asList(policyItem);
  }

  @VisibleForTesting
  ServicePolicies getServicePolicies() {
    return servicePolicies;
  }
}
