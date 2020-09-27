/*
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.TableProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import static org.junit.Assert.*;

/**
 * Class to test HBaseHbck. Spins up the minicluster once at test start and then takes it down
 * afterward. Add any testing of HBaseHbck functionality here.
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestHbck2 {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestHbck2.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHbck2.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf(TestHbck.class.getSimpleName());



  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniHBaseCluster(StartMiniClusterOption.builder().numMasters(2)
      .numRegionServers(3).build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(3);
  }

  public static class BypassChildProcedure extends ProcedureTestingUtility.NoopProcedure<MasterProcedureEnv> implements TableProcedureInterface {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
      while(EnvironmentEdgeManager.currentTime() < EnvironmentEdgeManager.currentTime()) {
        try {
          TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
        } catch (Exception e) {

        }
      }
      return null;
    }


    @Override
    public TableName getTableName() {
      return TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.READ;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected void setParentProcId(long parentProcId) {
      super.setParentProcId(parentProcId);
    }
  }

  public static class BypassParentProcedure extends ProcedureTestingUtility.NoopProcedure<MasterProcedureEnv> implements TableProcedureInterface {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
      return new Procedure[]{ new BypassChildProcedure()};
    }

    @Override
    public TableName getTableName() {
      return TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return TableOperationType.READ;
    }

  }

  @Test
  public void testBypassProcedurePersistence() throws Exception {

    BypassParentProcedure bypassParentProcedure = new BypassParentProcedure();
    long parentPid = bypassParentProcedure.getProcId();

    TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().submitProcedure(bypassParentProcedure);
    long bypassParent = bypassParentProcedure.getProcId();
    assertNotEquals(-1, bypassParent);
    TEST_UTIL.getHbck().bypassProcedure(Collections.singletonList(childPid),
      30000, false, false);
    TEST_UTIL.getHBaseCluster().getMaster().stopMaster();
    TEST_UTIL.waitFor(100000, () -> TEST_UTIL.getHBaseCluster().getLiveMasterThreads().size() == 1);

    //TEST_UTIL.getHBaseCluster().startMaster();
    while(!TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(TimeUnit.MINUTES.toMillis(2))) {
      Thread.sleep(1000);
    }
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    List<Procedure<?>> procedures = master.getProcedures();
    assertNotNull(procedures);
    int count = 0;
    for (Procedure<?> procedure : procedures) {
      long pid = procedure.getProcId();
      if (bypassPids.contains(pid)) {
        ++count;
        assertTrue(((pid == childPid) ?  "Bypassed "  + "child " : "parent ")
          + "procedure is not persistent", procedure.isBypass() && procedure.isSuccess());
      }
    }
    assertEquals("Procedure count is not equals to  bypass", 2, count);
  }
}
