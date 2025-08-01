/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.broker;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.NotAuthorizedException;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.common.config.provider.AccessControlUserCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.BcryptUtils;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.BasicAuthUtils;
import org.apache.pinot.core.auth.ZkBasicAuthPrincipal;
import org.apache.pinot.spi.auth.AuthorizationResult;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.broker.RequesterIdentity;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Zookeeper Basic Authentication based on Pinot Controller UI.
 * The user role has been distinguished by user and admin. Only admin can have access to the
 * user console page in Pinot controller UI. And admin can change user info (table permission/
 * number of tables/password etc.) or add/delete user without restarting your Pinot clusters,
 * and these changes happen immediately.
 * Users Configuration store in Helix Zookeeper and encrypted user password via Bcrypt Encryption Algorithm.
 *
 */
public class ZkBasicAuthAccessControlFactory extends AccessControlFactory {

  private AccessControl _accessControl;

  public ZkBasicAuthAccessControlFactory() {
    // left blank
  }

  @Override
  public void init(PinotConfiguration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _accessControl = new BasicAuthAccessControl(new AccessControlUserCache(propertyStore));
  }

  @Override
  public AccessControl create() {
    return _accessControl;
  }

  /**
   * Access Control using header-based basic http authentication
   */
  private static class BasicAuthAccessControl implements AccessControl {
    private Map<String, ZkBasicAuthPrincipal> _name2principal;
    private final AccessControlUserCache _userCache;

    public BasicAuthAccessControl(AccessControlUserCache userCache) {
      _userCache = userCache;
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity) {
      return authorize(requesterIdentity, (BrokerRequest) null);
    }

    @Override
    public AuthorizationResult authorize(RequesterIdentity requesterIdentity, BrokerRequest brokerRequest) {
      if (brokerRequest == null || !brokerRequest.isSetQuerySource() || !brokerRequest.getQuerySource()
          .isSetTableName()) {
        // no table restrictions? accept
        return TableAuthorizationResult.success();
      }

      return authorize(requesterIdentity, Collections.singleton(brokerRequest.getQuerySource().getTableName()));
    }

    @Override
    public TableAuthorizationResult authorize(RequesterIdentity requesterIdentity, Set<String> tables) {
      Optional<ZkBasicAuthPrincipal> principalOpt = getPrincipalAuth(requesterIdentity);
      if (!principalOpt.isPresent()) {
        throw new NotAuthorizedException("Basic");
      }
      if (tables == null || tables.isEmpty()) {
        return TableAuthorizationResult.success();
      }

      ZkBasicAuthPrincipal principal = principalOpt.get();
      Set<String> failedTables = new HashSet<>();
      for (String table : tables) {
        if (!principal.hasTable(TableNameBuilder.extractRawTableName(table))) {
          failedTables.add(table);
        }
      }
      if (failedTables.isEmpty()) {
        return TableAuthorizationResult.success();
      }
      return new TableAuthorizationResult(failedTables);
    }

    private Optional<ZkBasicAuthPrincipal> getPrincipalAuth(RequesterIdentity requesterIdentity) {
      Collection<String> tokens = extractAuthorizationTokens(requesterIdentity);
      if (tokens.isEmpty()) {
        return Optional.empty();
      }

      _name2principal = BasicAuthUtils.extractBasicAuthPrincipals(_userCache.getAllBrokerUserConfig()).stream()
          .collect(Collectors.toMap(BasicAuthPrincipal::getName, p -> p));

      Map<String, String> name2password = tokens.stream().collect(
          Collectors.toMap(org.apache.pinot.common.auth.BasicAuthUtils::extractUsername,
              org.apache.pinot.common.auth.BasicAuthUtils::extractPassword));
      Map<String, ZkBasicAuthPrincipal> password2principal =
          name2password.keySet().stream().collect(Collectors.toMap(name2password::get, _name2principal::get));

      return password2principal.entrySet().stream().filter(
          entry -> BcryptUtils.checkpwWithCache(entry.getKey(), entry.getValue().getPassword(),
              _userCache.getUserPasswordAuthCache())).map(u -> u.getValue()).filter(Objects::nonNull).findFirst();
    }
  }
}
