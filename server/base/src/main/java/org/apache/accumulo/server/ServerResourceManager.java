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

import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;

/**
 * Manages various server-side resources such as the TableManager, UniqueNameAllocator,
 * and ZooReaderWriter for the ServerContext.
 */
public class ServerResourceManager {

  private final ServerContext context;
  private final ServerInfo info;
  private TableManager tableManager;
  private UniqueNameAllocator nameAllocator;
  private final ZooReaderWriter zooReaderWriter;
  private ServerConfigurationFactory serverConfFactory;

  /**
   * Constructs a new ServerResourceManager.
   *
   * @param context the server context
   * @param info the server info
   * @param zooReaderWriter the ZooKeeper reader/writer
   */
  public ServerResourceManager(ServerContext context, ServerInfo info,
      ZooReaderWriter zooReaderWriter) {
    this.context = context;
    this.info = info;
    this.zooReaderWriter = zooReaderWriter;
  }

  /**
   * Gets the TableManager instance.
   *
   * @return the table manager
   */
  public synchronized TableManager getTableManager() {
    if (tableManager == null) {
      tableManager = new TableManager(context);
    }
    return tableManager;
  }

  /**
   * Gets the UniqueNameAllocator instance.
   *
   * @return the unique name allocator
   */
  public synchronized UniqueNameAllocator getUniqueNameAllocator() {
    if (nameAllocator == null) {
      nameAllocator = new UniqueNameAllocator(context);
    }
    return nameAllocator;
  }

  /**
   * Gets the ZooReaderWriter instance.
   *
   * @return the zoo reader/writer
   */
  public ZooReaderWriter getZooReaderWriter() {
    return zooReaderWriter;
  }

  /**
   * Gets the ServerConfigurationFactory instance.
   *
   * @return the server configuration factory
   */
  public synchronized ServerConfigurationFactory getServerConfFactory() {
    if (serverConfFactory == null) {
      serverConfFactory = new ServerConfigurationFactory(context, info.getSiteConfiguration());
    }
    return serverConfFactory;
  }
}
