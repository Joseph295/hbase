package org.apache.hadoop.hbase.regionserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "deprecation"})
public class RegionServer extends Thread implements
    RegionServerServices, LastSequenceId, ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServer.class);
  /**
   * For testing only!  Set to true to skip notifying region assignment to master .
   */
  @VisibleForTesting
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="MS_SHOULD_BE_FINAL")
  public static boolean TEST_SKIP_REPORTING_TRANSITION = false;


  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  private final Map<String, HRegion> onlineRegions = new ConcurrentHashMap<>();

  /**
   * Lock for gating access to {@link #onlineRegions}.
   * TODO: If this map is gated by a lock, does it need to be a ConcurrentHashMap?
   */
  private final ReentrantReadWriteLock onlineRegionsLock = new ReentrantReadWriteLock();

  /**
   * A map from RegionName to current action in progress. Boolean value indicates:
   * true - if open region action in progress
   * false - if close region action in progress
   */
  private final ConcurrentMap<byte[], Boolean> regionsInTransition =
      new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);


  /**
   * Used to cache the open/close region procedures which already submitted.
   * See {@link #submitRegionProcedure(long)}.
   */
  private final ConcurrentMap<Long, Long> submittedRegionProcedures = new ConcurrentHashMap<>();
  /**
   * Used to cache the open/close region procedures which already executed.
   * See {@link #submitRegionProcedure(long)}.
   */
  private final Cache<Long, Long> executedRegionProcedures =
      CacheBuilder.newBuilder().expireAfterAccess(600, TimeUnit.SECONDS).build();

  /**
   * Used to cache the moved-out regions
   */
  private final Cache<String, HRegionServer.MovedRegionInfo> movedRegionInfoCache =
      CacheBuilder.newBuilder().expireAfterWrite(movedRegionCacheExpiredTime(),
          TimeUnit.MILLISECONDS).build();

  private MemStoreFlusher cacheFlusher;

  private HeapMemoryManager heapMemoryManager;

  /**
   * The asynchronous cluster connection to be shared by services.
   */
  protected AsyncClusterConnection asyncClusterConnection;


  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  private ReplicationSourceService replicationSourceHandler;
  private ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplit compactSplitThread;

}
