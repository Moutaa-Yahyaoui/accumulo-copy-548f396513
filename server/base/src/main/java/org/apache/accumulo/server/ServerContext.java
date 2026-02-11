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

import java.util.Objects;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.ClientInfo;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.crypto.CryptoServiceFactory.ClassloaderType;
import org.apache.accumulo.core.rpc.SslConnectionParams;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.rpc.SaslServerConnectionParams;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a server context for Accumulo server components that operate with the system credentials
 * and have access to the system files and configuration.
 *
 * <p>
 * This class has been refactored to delegate focused responsibilities to:
 * <ul>
 * <li>{@link ServerSecurityProcessor} for security-related operations</li>
 * <li>{@link ServerRpcConfiguration} for RPC-related configuration</li>
 * <li>{@link ServerResourceManager} for server resource management</li>
 * </ul>
 */
public class ServerContext extends ClientContext {

  private static final Logger log = LoggerFactory.getLogger(ServerContext.class);

  private final ServerInfo info;
  private final ServerResourceManager resourceManager;
  private final ServerSecurityProcessor securityProcessor;
  private final ServerRpcConfiguration rpcConfig;

  private String applicationName = null;
  private String applicationClassName = null;
  private String hostname = null;
  private CryptoService cryptoService = null;

  public ServerContext(SiteConfiguration siteConfig) {
    this(new ServerInfo(siteConfig));
  }

  public ServerContext(SiteConfiguration siteConfig, String instanceName, String zooKeepers,
      int zooKeepersSessionTimeOut) {
    this(new ServerInfo(siteConfig, instanceName, zooKeepers, zooKeepersSessionTimeOut));
  }

  public ServerContext(SiteConfiguration siteConfig, Properties clientProps) {
    this(siteConfig, ClientInfo.from(clientProps));
  }

  private ServerContext(SiteConfiguration siteConfig, ClientInfo info) {
    this(new ServerInfo(siteConfig, info.getInstanceName(), info.getZooKeepers(),
        info.getZooKeepersSessionTimeOut()));
  }

  private ServerContext(ServerInfo info) {
    super(info, info.getSiteConfiguration());
    this.info = info;
    this.resourceManager = new ServerResourceManager(this, info,
        new ZooReaderWriter(info.getSiteConfiguration()));
    this.securityProcessor = new ServerSecurityProcessor();
    this.rpcConfig = new ServerRpcConfiguration();
  }

  public void setupServer(String appName, String appClassName, String hostname) {
    applicationName = appName;
    applicationClassName = appClassName;
    this.hostname = hostname;
    SecurityUtil.serverLogin(info.getSiteConfiguration());
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + info.getInstanceID());
    ServerUtil.init(this, applicationName);
    MetricsSystemHelper.configure(applicationClassName);
    TraceUtil.enableServerTraces(hostname, applicationName,
        getServerConfFactory().getSystemConfiguration());
    if (getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      enforceKerberosLogin();
    }
  }

  /**
   * Should only be called by the Tablet server
   */
  public synchronized void setupCrypto() throws CryptoService.CryptoException {
    if (cryptoService != null)
      throw new CryptoService.CryptoException("Crypto Service " + cryptoService.getClass().getName()
          + " already exists and cannot be setup again");

    AccumuloConfiguration acuConf = getConfiguration();
    cryptoService = CryptoServiceFactory.newInstance(acuConf, ClassloaderType.ACCUMULO);
  }

  public void teardownServer() {
    TraceUtil.disable();
  }

  public String getHostname() {
    Objects.requireNonNull(hostname);
    return hostname;
  }

  public ServerConfigurationFactory getServerConfFactory() {
    return resourceManager.getServerConfFactory();
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    return getServerConfFactory().getSystemConfiguration();
  }

  /**
   * A "client-side" assertion for servers to validate that they are logged in as the expected user,
   * per the configuration, before performing any RPC
   */
  // Should be private, but package-protected so EasyMock will work
  void enforceKerberosLogin() {
    securityProcessor.enforceKerberosLogin(getServerConfFactory().getSiteConfiguration());
  }

  public VolumeManager getVolumeManager() {
    return info.getVolumeManager();
  }

  public ZooReaderWriter getZooReaderWriter() {
    return resourceManager.getZooReaderWriter();
  }

  /**
   * Retrieve the SSL/TLS configuration for starting up a listening service
   */
  public SslConnectionParams getServerSslParams() {
    return rpcConfig.getServerSslParams(getConfiguration());
  }

  @Override
  public SaslServerConnectionParams getSaslParams() {
    return securityProcessor.getSaslParams(getServerConfFactory().getSiteConfiguration(),
        getCredentials());
  }

  /**
   * Determine the type of Thrift server to instantiate given the server's configuration.
   *
   * @return A {@link ThriftServerType} value to denote the type of Thrift server to construct
   */
  public ThriftServerType getThriftServerType() {
    return rpcConfig.getThriftServerType(getConfiguration());
  }

  public void setSecretManager(AuthenticationTokenSecretManager secretManager) {
    securityProcessor.setSecretManager(secretManager);
  }

  public AuthenticationTokenSecretManager getSecretManager() {
    return securityProcessor.getSecretManager();
  }

  public TableManager getTableManager() {
    return resourceManager.getTableManager();
  }

  public UniqueNameAllocator getUniqueNameAllocator() {
    return resourceManager.getUniqueNameAllocator();
  }

  public CryptoService getCryptoService() {
    if (cryptoService == null) {
      throw new CryptoService.CryptoException("Crypto service not initialized.");
    }
    return cryptoService;
  }
}
