/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;

import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Handles security-related operations for the ServerContext, such as Kerberos login
 * enforcement and SASL configuration.
 */
public class ServerSecurityProcessor {

  private AuthenticationTokenSecretManager secretManager;

  public ServerSecurityProcessor() {}

  /**
   * Sets the secret manager for authentication tokens.
   *
   * @param secretManager the secret manager
   */
  public void setSecretManager(AuthenticationTokenSecretManager secretManager) {
    this.secretManager = secretManager;
  }

  /**
   * Gets the secret manager for authentication tokens.
   *
   * @return the secret manager
   */
  public AuthenticationTokenSecretManager getSecretManager() {
    return secretManager;
  }

  /**
   * A "client-side" assertion for servers to validate that they are logged in as the expected user,
   * per the configuration, before performing any RPC.
   *
   * @param conf the configuration
   */
  public void enforceKerberosLogin(AccumuloConfiguration conf) {
    // Unwrap _HOST into the FQDN to make the kerberos principal we'll compare against
    final String kerberosPrincipal = SecurityUtil
        .getServerPrincipal(conf.get(Property.GENERAL_KERBEROS_PRINCIPAL));
    UserGroupInformation loginUser;
    try {
      // The system user should be logged in via keytab when the process is started, not the
      // currentUser() like KerberosToken
      loginUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new RuntimeException("Could not get login user", e);
    }

    checkArgument(loginUser.hasKerberosCredentials(), "Server does not have Kerberos credentials");
    checkArgument(kerberosPrincipal.equals(loginUser.getUserName()),
        "Expected login user to be " + kerberosPrincipal + " but was " + loginUser.getUserName());
  }

  /**
   * Gets the SASL parameters for the server.
   *
   * @param conf the configuration
   * @param credentials the server credentials
   * @return the SASL parameters, or null if SASL is not enabled
   */
  public SaslServerConnectionParams getSaslParams(AccumuloConfiguration conf, Credentials credentials) {
    if (!conf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      return null;
    }
    return new SaslServerConnectionParams(conf, credentials.getToken(), secretManager);
  }
}
