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
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
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
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setUp() throws IOException {
    TEST_UTIL.ensureSomeRegionServersAvailable(3);
  }

  public static class BypassChildProcedure extends ProcedureTestingUtility.NoopProcedure<MasterProcedureEnv> implements TableProcedureInterface {
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    protected Procedure[] execute(final MasterProcedureEnv env) throws ProcedureSuspendedException {
      setTimeout(10000);
      try {
        Thread.sleep(20000);
      } catch (Exception e) {

      }
      throw new ProcedureSuspendedException();
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

    @Override
    protected void completionCleanup(final MasterProcedureEnv env) {
      HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
      master.stop("We stop here before the parent procedure stop");

      assertEquals("Unexpected master threads", 1, TEST_UTIL.getHBaseCluster().getLiveMasterThreads().size());
      try {
        TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster(TimeUnit.MINUTES.toMillis(2));
      } catch (IOException e) {
        LOG.error("Exception when waitForActiveAndReadyMaster", e);
      }
      assertNotEquals("The active master is not changed", master, TEST_UTIL.getHBaseCluster().getMaster());
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
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    long ppid = master.getMasterProcedureExecutor().submitProcedure(bypassParentProcedure);
    Thread.sleep(1000);
    List<Procedure<?>> procedures = master.getProcedures();
    long pid = -1;
    for (Procedure<?> procedure : procedures) {
      if (procedure.getParentProcId() == ppid) {
        pid = procedure.getProcId();
      }
    }
    assertNotEquals("", -1, pid);
    //assertEquals("Unexpected procedure count!", 2, procedures.size());
    //Procedure<?> child = (procedures.get(0).getProcId() == ppid ? procedures.get(1) : procedures.get(0));
    TEST_UTIL.getHbck().bypassProcedure(Collections.singletonList(pid),
      30000, false, false);

    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    assertNotNull(master);
    procedures = master.getProcedures();
    for (Procedure<?> procedure : procedures) {
      if (procedure.getParentProcId() == ppid) {
        //assertEquals("", child.getProcId(), procedure.getProcId());
        assertTrue("pid=" + procedure.getProcId(), procedure.isBypass());
      } else {
        // assertTrue(procedure.isBypass());
      }
    }
    for (Procedure<?> procedure : procedures) {
      if (procedure.getProcId() == ppid) {
        //assertEquals("", child.getProcId(), procedure.getProcId());
        assertTrue("ppid=" + procedure.getProcId(), procedure.isBypass());
      } else {
        // assertTrue(procedure.isBypass());
      }
    }
  }
}
