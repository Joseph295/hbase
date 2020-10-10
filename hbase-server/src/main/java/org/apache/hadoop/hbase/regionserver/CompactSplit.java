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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.Store.NO_PRIORITY;
import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;
import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplit implements CompactionRequester, PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(CompactSplit.class);

  // Configuration key for the large compaction threads.
  public final static String LARGE_COMPACTION_THREADS =
      "hbase.regionserver.thread.compaction.large";
  public final static int LARGE_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for the small compaction threads.
  public final static String SMALL_COMPACTION_THREADS =
      "hbase.regionserver.thread.compaction.small";
  public final static int SMALL_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for split threads
  public final static String SPLIT_THREADS = "hbase.regionserver.thread.split";
  public final static int SPLIT_THREADS_DEFAULT = 1;

  public static final String REGION_SERVER_REGION_SPLIT_LIMIT =
      "hbase.regionserver.regionSplitLimit";
  public static final int DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT= 1000;
  public static final String HBASE_REGION_SERVER_ENABLE_COMPACTION =
      "hbase.regionserver.compaction.enabled";

  public final HRegionServer regionServer;
  public final Configuration conf;
  public volatile ThreadPoolExecutor longCompactionThreadPool;
  public volatile ThreadPoolExecutor shortCompactionThreadPool;
  public volatile ThreadPoolExecutor splitThreadPool;
  public volatile ThroughputController compactionThroughputController;
  public volatile boolean isCompactionEnabled;
  private int regionSplitSoftLimit;

  CompactSplit(HRegionServer regionServer) {
    this.regionServer = regionServer;
    this.conf = regionServer.getConfiguration();
    this.isCompactionEnabled = this.conf.getBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION,true);
    createCompactionExecutors();
    createSplitExcecutors();
    this.compactionThroughputController = CompactionThroughputControllerFactory.create(regionServer, conf);
  }

  private void createSplitExcecutors() {
    final String splitThreadName = Thread.currentThread().getName();
    this.splitThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(conf.getInt(SPLIT_THREADS, SPLIT_THREADS_DEFAULT),
      new ThreadFactoryBuilder().setNameFormat(splitThreadName + "-splits-%d").setDaemon(true).build());
  }

  private void createCompactionExecutors() {
    this.regionSplitSoftLimit =
        conf.getInt(REGION_SERVER_REGION_SPLIT_LIMIT, DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT);

    int largeThreads =
        Math.max(1, conf.getInt(LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
    int smallThreads = conf.getInt(SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);
    
    final String compactionThreadName = Thread.currentThread().getName();

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<Runnable>(COMPARATOR);
    this.longCompactionThreadPool = new ThreadPoolExecutor(largeThreads, largeThreads, 60, TimeUnit.SECONDS,
        stealJobQueue, new ThreadFactoryBuilder().setNameFormat(compactionThreadName + "-longCompactions-%d")
            .setDaemon(true).build());
    this.longCompactionThreadPool.setRejectedExecutionHandler(new Rejection());
    this.longCompactionThreadPool.prestartAllCoreThreads();
    this.shortCompactionThreadPool = new ThreadPoolExecutor(smallThreads, smallThreads, 60, TimeUnit.SECONDS,
        stealJobQueue.getStealFromQueue(), new ThreadFactoryBuilder()
            .setNameFormat(compactionThreadName + "-shortCompactions-%d").setDaemon(true).build());
    this.shortCompactionThreadPool.setRejectedExecutionHandler(new Rejection());
  }

  public synchronized boolean requestSplit(final Region r) {
    // don't split regions that are blocking
    HRegion hregion = (HRegion)r;
    try {
      if ((regionServer.getOnlineRegionCount() < regionSplitSoftLimit)
        && hregion.getCompactPriority() >= PRIORITY_USER) {
        byte[] midKey = hregion.checkSplit().orElse(null);
        if (midKey != null) {
          requestSplit(r, midKey);
          return true;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      // We get this sometimes. Not sure why. Catch and return false; no split request.
      LOG.warn("Catching out-of-bounds; region={}, policy={}", hregion == null? null: hregion.getRegionInfo(),
        hregion == null? "null": hregion.getCompactPriority(), e);
    }
    return false;
  }

  public synchronized void requestSplit(final Region r, byte[] midKey) {
    requestSplit(r, midKey, null);
  }

  /*
   * The User parameter allows the split thread to assume the correct user identity
   */
  public synchronized void requestSplit(final Region region, byte[] midKey, User user) {
    if (midKey == null) {
      LOG.debug("Region " + region.getRegionInfo().getRegionNameAsString() +
        " not splittable because midkey=null");
      return;
    }
    try {
      this.splitThreadPool.execute(new SplitRequest(region, midKey, this.regionServer, user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting " + region + ", " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + region, ree);
    }
  }

  private void interrupt() {
    longCompactionThreadPool.shutdownNow();
    shortCompactionThreadPool.shutdownNow();
  }

  private void reInitializeCompactionsExecutors() {
    createCompactionExecutors();
  }

  @Override
  public synchronized void requestRegionCompaction(HRegion region, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, why, priority, true, tracker, user);
  }

  @Override
  public synchronized void requestStoreCompaction(HRegion region, HStore store, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, store, why, priority, true, tracker, user);
  }

  @Override
  public void switchCompaction(boolean compactionSwitch) {
    if (compactionSwitch && !isCompactionEnabled) {
      LOG.info("Re-Initializing compactions because user switched on compactions");
      reInitializeCompactionsExecutors();
    } else {
      LOG.info("Interrupting running compactions because user switched off compactions");
      interrupt();
    }
    this.isCompactionEnabled = compactionSwitch;
    this.conf.set(HBASE_REGION_SERVER_ENABLE_COMPACTION,String.valueOf(isCompactionEnabled));
  }

  private void requestCompactionInternal(HRegion region, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker, User user) throws IOException {
    // request compaction on all stores
    for (HStore store : region.stores.values()) {
      requestCompactionInternal(region, store, why, priority, selectNow, tracker,
        user);
    }
  }

  private void requestCompactionInternal(HRegion region, HStore store, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker, User user) throws IOException {
    if (this.regionServer.isStopped() || (region.getTableDescriptor() != null &&
        !region.getTableDescriptor().isCompactionEnabled())) {
      return;
    }
    RegionServerSpaceQuotaManager spaceQuotaManager =
        this.regionServer.getRegionServerSpaceQuotaManager();

    if (user != null && !Superusers.isSuperUser(user) && spaceQuotaManager != null
        && spaceQuotaManager.areCompactionsDisabled(region.getTableDescriptor().getTableName())) {
      // Enter here only when:
      // It's a user generated req, the user is super user, quotas enabled, compactions disabled.
      String reason = "Ignoring compaction request for " + region +
          " as an active space quota violation " + " policy disallows compactions.";
      tracker.notExecuted(store, reason);
      LOG.debug(reason);
      return;
    }

    CompactionContext compaction;
    if (selectNow) {
      Optional<CompactionContext> c =
        selectCompaction(region, store, priority, user);
      if (!c.isPresent()) {
        // message logged inside
        return;
      }
      compaction = c.get();
    } else {
      compaction = null;
    }

    ThreadPoolExecutor pool;
    if (selectNow) {
      // compaction.get is safe as we will just return if selectNow is true but no compaction is
      // selected
      pool = store.throttleCompaction(compaction.getRequest().getSize()) ? longCompactionThreadPool
          : shortCompactionThreadPool;
    } else {
      // We assume that most compactions are small. So, put system compactions into small
      // pool; we will do selection there, and move to large pool if necessary.
      pool = shortCompactionThreadPool;
    }
    pool.execute(
      new CompactionRunner(store, region, compaction, pool, user));
    region.incrementCompactionsQueuedCount();
    if (LOG.isDebugEnabled()) {
      String type = (pool == shortCompactionThreadPool) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
          + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
  }

  public synchronized void requestSystemCompaction(HRegion region, String why) throws IOException {
    requestCompactionInternal(region, why, NO_PRIORITY, false, CompactionLifeCycleTracker.DUMMY,
      null);
  }

  public synchronized void requestSystemCompaction(HRegion region, HStore store, String why)
      throws IOException {
    requestCompactionInternal(region, store, why, NO_PRIORITY, false,
      CompactionLifeCycleTracker.DUMMY, null);
  }

  private Optional<CompactionContext> selectCompaction(HRegion region, HStore store, int priority,
      User user)
      throws IOException {
    // don't even select for compaction if disableCompactions is set to true
    if (!isCompactionEnabled) {
      LOG.info(String.format("User has disabled compactions"));
      return Optional.empty();
    }
    Optional<CompactionContext> compaction = store.requestCompaction(priority, user);
    if (!compaction.isPresent() && region.getRegionInfo() != null) {
      String reason = "Not compacting " + region.getRegionInfo().getRegionNameAsString() +
          " because compaction request was cancelled";
      LOG.debug(reason);
    }
    return compaction;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splitThreadPool.shutdown();
    longCompactionThreadPool.shutdown();
    shortCompactionThreadPool.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Waiting for " + name + " to finish...");
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
        t.shutdownNow();
      }
    }
  }

  void join() {
    waitFor(splitThreadPool, "Split Thread");
    waitFor(longCompactionThreadPool, "Large Compaction Thread");
    waitFor(shortCompactionThreadPool, "Small Compaction Thread");
  }

  private static final Comparator<Runnable> COMPARATOR =
      new Comparator<Runnable>() {
    private int compare(CompactionRequestImpl r1, CompactionRequestImpl r2) {
      if (r1 == r2) {
        return 0; //they are the same request
      }
      // less first
      int cmp = Integer.compare(r1.getPriority(), r2.getPriority());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(r1.getSelectionTime(), r2.getSelectionTime());
      if (cmp != 0) {
        return cmp;
      }

      // break the tie based on hash code
      return System.identityHashCode(r1) - System.identityHashCode(r2);
    }

    @Override
    public int compare(Runnable r1, Runnable r2) {
      // CompactionRunner first
      if (r1 instanceof CompactionRunner) {
        if (!(r2 instanceof CompactionRunner)) {
          return -1;
        }
      } else {
        if (r2 instanceof CompactionRunner) {
          return 1;
        } else {
          // break the tie based on hash code
          return System.identityHashCode(r1) - System.identityHashCode(r2);
        }
      }
      CompactionRunner o1 = (CompactionRunner) r1;
      CompactionRunner o2 = (CompactionRunner) r2;
      // less first
      int cmp = Integer.compare(o1.queuedPriority, o2.queuedPriority);
      if (cmp != 0) {
        return cmp;
      }
      CompactionContext c1 = o1.compactionContext;
      CompactionContext c2 = o2.compactionContext;
      if (c1 != null) {
        return c2 != null ? compare(c1.getRequest(), c2.getRequest()) : -1;
      } else {
        return c2 != null ? 1 : 0;
      }
    }
  };

  public final class CompactionRunner implements Runnable {
    private final HStore store;
    private final HRegion region;
    private final CompactionContext compactionContext;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;

    public CompactionRunner(HStore store, HRegion region, CompactionContext compactionContext,
        ThreadPoolExecutor parent, User user) {
      this.store = store;
      this.region = region;
      this.compactionContext = compactionContext;
      this.queuedPriority =
          compactionContext != null ? compactionContext.getRequest().getPriority() : store.getCompactPriority();
      this.parent = parent;
      this.user = user;
    }

    private void doCompaction(User user) {
      CompactionContext c;
      // Common case - system compaction without a file selection. Select now.
      if (compactionContext == null) {
        int oldPriority = this.queuedPriority;
        this.queuedPriority = this.store.getCompactPriority();
        if (this.queuedPriority > oldPriority) {
          // Store priority decreased while we were in queue (due to some other compaction?),
          // requeue with new priority to avoid blocking potential higher priorities.
          this.parent.execute(this);
          return;
        }
        Optional<CompactionContext> selected;
        try {
          selected = selectCompaction(this.region, this.store, queuedPriority, user);
        } catch (IOException ex) {
          LOG.error("Compaction selection failed " + this, ex);
          regionServer.checkFileSystem();
          region.decrementCompactionsQueuedCount();
          return;
        }
        if (!selected.isPresent()) {
          region.decrementCompactionsQueuedCount();
          return; // nothing to do
        }
        c = selected.get();
        assert c.hasSelection();
        // Now see if we are in correct pool for the size; if not, go to the correct one.
        // We might end up waiting for a while, so cancel the selection.

        ThreadPoolExecutor pool =
            store.throttleCompaction(c.getRequest().getSize()) ?
              longCompactionThreadPool : shortCompactionThreadPool;

        // Long compaction pool can process small job
        // Short compaction pool should not process large job
        if (this.parent == shortCompactionThreadPool && pool == longCompactionThreadPool) {
          this.store.cancelRequestedCompaction(c);
          this.parent = pool;
          this.parent.execute(this);
          return;
        }
      } else {
        c = compactionContext;
      }
      // Finally we can compact something.
      assert c != null;

      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTime();
        boolean completed =
            region.compact(c, store, compactionThroughputController, user);
        long now = EnvironmentEdgeManager.currentTime();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (store.getCompactPriority() <= 0) {
            requestSystemCompaction(region, store, "Recursive enqueue");
          } else {
            // see if the compaction has caused us to exceed max region size
            requestSplit(region);
          }
        }
      } catch (Exception ex) {
        Exception remoteEx =
            ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex;
        LOG.error("Compaction failed " + this, remoteEx);
        region.reportCompactionRequestFailure();
        regionServer.checkFileSystem();
      } finally {
        region.decrementCompactionsQueuedCount();
        LOG.debug("Status {}", CompactSplit.this);
      }
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(regionServer);
      if (regionServer.isStopped() || (region.getTableDescriptor() != null &&
        !region.getTableDescriptor().isCompactionEnabled())) {
        region.decrementCompactionsQueuedCount();
        return;
      }
      doCompaction(user);
    }
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private static class Rejection implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor pool) {
      if (runnable instanceof CompactionRunner) {
        CompactionRunner runner = (CompactionRunner) runnable;
        LOG.debug("Compaction Rejected: " + runner);
        if (runner.compactionContext != null) {
          runner.store.cancelRequestedCompaction(runner.compactionContext);
        }
      }
    }
  }
}
