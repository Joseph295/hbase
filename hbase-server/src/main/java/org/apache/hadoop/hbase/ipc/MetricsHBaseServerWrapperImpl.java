/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.util.DirectMemoryUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsHBaseServerWrapperImpl implements MetricsHBaseServerWrapper {

  private RpcServer rpcServer;

  MetricsHBaseServerWrapperImpl(RpcServer rpcServer) {
    this.rpcServer = rpcServer;
  }

  private boolean isServerStarted() {
    return this.rpcServer != null && this.rpcServer.isStarted();
  }

  @Override
  public long getTotalQueueSize() {
    if (!isServerStarted()) {
      return 0;
    }
    return rpcServer.callQueueSizeInBytes.sum();
  }

  @Override
  public int getGeneralQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getGeneralQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getReplicationQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getPriorityQueueLength();
  }

  @Override
  public int getMetaPriorityQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getMetaPriorityQueueLength();
  }

  @Override
  public int getNumOpenConnections() {
    if (!isServerStarted()) {
      return 0;
    }
    return rpcServer.getNumOpenConnections();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveRpcHandlerCount();
  }

  @Override
  public int getActiveGeneralRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveGeneralRpcHandlerCount();
  }

  @Override
  public int getActivePriorityRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActivePriorityRpcHandlerCount();
  }

  @Override
  public int getActiveMetaPriorityRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveMetaPriorityRpcHandlerCount();
  }

  @Override
  public int getActiveReplicationRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveReplicationRpcHandlerCount();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getNumGeneralCallsDropped();
  }

  @Override
  public long getNumLifoModeSwitches() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getNumLifoModeSwitches();
  }

  @Override
  public int getWriteQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveWriteRpcHandlerCount();
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveReadRpcHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0;
    }
    return rpcServer.getRpcScheduler().getActiveScanRpcHandlerCount();
  }

  @Override
  public long getNettyDmUsage() {
    if (!isServerStarted() || this.rpcServer.getRpcScheduler() == null) {
      return 0L;
    }

    return DirectMemoryUtils.getNettyDirectMemoryUsage();
  }
}
