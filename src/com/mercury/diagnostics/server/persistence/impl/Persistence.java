/*
 * (c) Copyright 2008 - 2013 Hewlett-Packard Development Company, L.P. 
 * --------------------------------------------------------------------------
 * This file contains proprietary trade secrets of Hewlett Packard Company.
 * No part of this file may be reproduced or transmitted in any form or by
 * any means, electronic or mechanical, including photocopying and
 * recording, for any purpose without the expressed written permission of
 * Hewlett Packard Company.
 */


package com.mercury.diagnostics.server.persistence.impl;


import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.ThreadFactory;
import com.mercury.diagnostics.common.annotation.PicoConstructor;
import com.mercury.diagnostics.common.annotation.PicoReference;
import com.mercury.diagnostics.common.api.InstrumentationPoint;
import com.mercury.diagnostics.common.data.graph.*;
import com.mercury.diagnostics.common.data.graph.impl_oo.BucketedAggregateTreeNode;
import com.mercury.diagnostics.common.data.graph.impl_oo.StoredTreeRecord;
import com.mercury.diagnostics.common.data.graph.impl_oo.WritableReader;
import com.mercury.diagnostics.common.data.graph.node.*;
import com.mercury.diagnostics.common.data.graph.query.CachedQueryKey;
import com.mercury.diagnostics.common.data.graph.query.IQuery;
import com.mercury.diagnostics.common.io.INodeData;
import com.mercury.diagnostics.common.io.NodeDataFactory;
import com.mercury.diagnostics.common.io.NodeIdentifier;
import com.mercury.diagnostics.common.loader.AbstractModuleObject;
import com.mercury.diagnostics.common.loader.IModule;
import com.mercury.diagnostics.common.logging.Level;
import com.mercury.diagnostics.common.logging.Loggers;
import com.mercury.diagnostics.common.modules.monitoring.*;
import com.mercury.diagnostics.common.util.DynamicPropertiesUtil;
import com.mercury.diagnostics.common.util.InfrequentEventScheduler;
import com.mercury.diagnostics.common.util.InfrequentLogger;
import com.mercury.diagnostics.common.util.PropertiesUtil;
import com.mercury.diagnostics.common.util.collections.EmptyIterator;
import com.mercury.diagnostics.common.util.collections.MRUHashMap;
import com.mercury.diagnostics.common.util.resource.IResourceManager;
import com.mercury.diagnostics.common.util.resource.ResourceManager;
import com.mercury.diagnostics.server.persistence.*;
import com.mercury.diagnostics.server.persistence.symboltable.impl.SymbolManager;
import com.mercury.diagnostics.server.resources.ResourceManagerKey;
import com.mercury.diagnostics.server.time.ServerTimeManager;
import com.mercury.opal.mediator.api.IStatusNodeFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * Persistence is the class that manages incoming data for persistence. The
 * algorithms for storing data are implemented here.
 *
 * TODO - the locking for creation/deletion is kind of strewn between this class and
 *        reaggregation manager. We should create an abstraction for this and clean
 *        up the code that implements it.
 */
public class Persistence extends AbstractModuleObject
  implements IMetricProvider, IPersistence {

  private final @PicoReference IPurger purger;
  private final @PicoReference QueryLock queryLock;
  private final @PicoReference CompressionBlocker compressionBlocker;

  /**
   * Factory for creating status nodes.
   */
  private IStatusNodeFactory statusNodeFactory;

  /**
   * The symbol manager object
   */
  private ISymbolManager symbolManager;

  /**
   * Manages the archive
   */
  StorageManager storageManager;

  /**
   * Time Manager
   */
  private ServerTimeManager timeManager;

  /**
   * Reaggregation Manager runs on InfrequentEvent checks whether/what needs to be reaggregated, then causing the actual reaggegation...
   */
  private ReaggregationManager reaggregationManager;

  /**
   * The infrequent scheduler used to schedule reaggregation
   */
  private InfrequentEventScheduler reagEventScheduler;

  /**
   * A comparator that gives BFS path order for path keys
   */
  private PathComparator nodeHolderComparator;

  /**
   * A simple cache of queries and their results. This is currently used only
   * in conjunction with persistence, to improve performance of repeated queries;
   */
  private final MRUHashMap/*<CachedQueryKey, QueryProcessor.Cached>*/ queryResultCache = new MRUHashMap();

  /**
   * convinience string to read from configuration file
   */
  static final String PERSISTENCY_MAJOR_PREFIX = "persistency.major.";

  private static final String UT_PATHED_FILE_NAME = "UT_PathSymbolTable.dat";

  /**
   * As we are writing out a new summary file, how many NodeHolders should we process
   * trends for at a given time. Every recordChunk NodeHolders the trends are written
   * to the trend file, and then the trend offset on those NodeHolders is updated in
   * the summary file.
   */
  private int recordChunk;

  private InstanceTreeSelector instanceTreeSelector;

  private volatile boolean permanentSevereCondition = false;

  private IResourceManager resourceManager;

  /**
   * the executor that processes the asynchronous side of bucket complete aggregation
   */
  private Executor bucketCompleteExecutor;

  /** used to enforce that onlineBucketComplete finishes before the next call to it starts */
  private final Object obcLock = new Object();

  /**
   * A blocker to make reaggregation of lower priority than asynchBucketComplete
   */
  private ProcessBlocker reaggregationBlocker = new ProcessBlocker();

  static final String MINOR_CONST = ".minor.";
  static final String RETENTION_CONST = ".retention";
  static final String LENGTH_CONST = ".length";

  /** the metrics for persistence */
  private PersistenceMetrics metrics;

  /** Stops and starts compression for the archives  */
  private boolean allowCompression = true;

  /** batch append (and eventually flush) tree blocks instead of write */
  private static final boolean BATCH_APPEND_TREES_DEFAULT = false;

  /**
   * Performance improvement(?) to append trees in batches and eventually flush.  Instead of individual writes.
   */
  private volatile boolean batchAppendTrees = BATCH_APPEND_TREES_DEFAULT;

  /** When latency is missing look for it by name because locale issues may have messed up the units */
  private boolean localeIndependentLatency = false;
  
  /** Do a breadth first search for NodeData queries. This performs much better when summary files are compressed */
  public boolean breadthFirstForNodeDataQuery = true;

  /** Stops and starts purging for the archives */
  private boolean disableOnlineDataPersistence = false;

  private DynamicPropertiesUtil dynamicProperties;

  /**
   * a value >= 1.0 that is used to increase the persistence topN for cache
   * performance.
   */
  private double topNMultiplier;
  
  /** Number of ms to delay flushing of NodeHolders */
  private Integer bucketWriteDelay = 0;
  
  /** At which major should we cut off tree aggregation. Useful to limit tree files in the higher majors (Year) */
  private String treeCutOffMajor = null;
  
  /**
   * This is the max chunk size we process when doing BFS in QueryProcessor
   */
  private int bfsChunkSize = 50000;

  public int getBfsChunkSize() {
    return bfsChunkSize;
  }



  /**
   * For UT
   */
  static Persistence createUTInstance() throws Exception {
    File pathedFile;
    Persistence persistence = new Persistence(null, new QueryLock(),
      new CompressionBlocker());
    PropertiesUtil properties = new PropertiesUtil();

    properties.setProperty("persistency.major.durations.num", "1");
    properties.setProperty("persistency.major.0.length", "5");
    properties.setProperty("persistency.major.0.unit", "m");
    properties.setProperty("persistency.major.0.retention", "5");
    properties.setProperty("persistency.major.0.datapoints", "80");
    properties.setProperty("persistency.major.0.name", "Hours");
    properties.setProperty("persistency.major.0.minors", "1");
    properties.setProperty("persistency.major.0.minor.0.name", "5m");
    properties.setProperty("persistency.major.0.minor.0.length", "5");
    properties.setProperty("persistency.major.0.minor.0.unit", "m");
    persistence.timeManager = ServerTimeManager.createUTInstance(properties);

    //LH - we MUST use an absolute path or we will end up loading ModuleLoader
    //     and the UT's that depend on this will fail.
    File persistenceDir = File.createTempFile("ut_persistence", ".dir");

    if (persistenceDir.exists()) {
      persistenceDir.delete();
    }
    persistenceDir.mkdirs();
    properties.setProperty(StorageManager.PERSISTENCY_DIR_PROPERTY, persistenceDir.getAbsolutePath());

    pathedFile = new File(UT_PATHED_FILE_NAME);
    pathedFile.delete();
    persistence.symbolManager = new SymbolManager(pathedFile.getAbsolutePath(), null, 10);
    persistence.storageManager = StorageManager.createUTInstance(properties);
    persistence.storageManager.setSymbolManager(persistence.symbolManager);
    persistence.nodeHolderComparator = new PathComparator(persistence.symbolManager);

    return persistence;
  }



  /**
   * Creates an instance of persistence for dumper utility
   * @param properties
   * @return
   * @throws Exception
   */
  public static Persistence createDumperInstance(PropertiesUtil properties) throws Exception {
    Persistence persistence = new Persistence(null, new QueryLock(),
      new CompressionBlocker());

    persistence.timeManager = ServerTimeManager.createUTInstance(properties);

    persistence.allowCompression = false;

    persistence.resourceManager = ResourceManager.getResourceManager(ResourceManagerKey.class);

    persistence.recordChunk = properties.getProperty("persistency.record_chunk", 10000);

    persistence.storageManager = StorageManager.createDumperInstance(properties, persistence.timeManager);
    persistence.symbolManager = SymbolManager.getDumperInstance(properties, persistence.storageManager, persistence.timeManager);

    // Ugh.  Darn dependencies.
    persistence.storageManager.setSymbolManager(persistence.symbolManager);

    TrendFile.initializeTransposeBufferCache(persistence.getMaxRecordLength(persistence.storageManager.getMajorDurations()));

    //  IMonitor monitor = null;
    //    MonitorModule monitorModule = (MonitorModule)module.getModuleLoader().getModule(MonitorModule.class);

    persistence.metrics = null;

    persistence.nodeHolderComparator = new PathComparator(persistence.symbolManager);

    persistence.statusNodeFactory = null; //todo?

    persistence.reaggregationManager = null;

    return persistence;
  }



  @PicoConstructor
  public Persistence(IPurger purger, QueryLock queryLock,
    CompressionBlocker compressionBlocker) {

    resourceManager = ResourceManager.getResourceManager(ResourceManagerKey.class);
    this.purger = purger;
    this.queryLock = queryLock;
    this.compressionBlocker = compressionBlocker;
  }



  public void initialize(IModule module) {
    metrics = new PersistenceMetrics(resourceManager);

    // We need our own security manager to prevent windows from barking
    // on mmap clean.
    SecurityManager prevManager = System.getSecurityManager();

    System.setSecurityManager(new PersistenceSecurityManager(prevManager));

    PropertiesUtil properties = module.getModuleProperties();

    allowCompression = properties.getProperty("persistence.allow.compression", true);
    batchAppendTrees = properties.getProperty("persistence.batch.append.trees", BATCH_APPEND_TREES_DEFAULT);
    disableOnlineDataPersistence = properties.getProperty("persistence.disable.onlinedatapersistence", false);
    localeIndependentLatency = properties.getProperty("persistence.enable.locale.independent.latency", false);
    breadthFirstForNodeDataQuery = properties.getProperty("persistence.enable.bfs.for.nodedata_query", true);
    bucketWriteDelay = Integer.getInteger("persistence.bucket.write.delay");
    if (bucketWriteDelay == null) bucketWriteDelay = 0;
    treeCutOffMajor = properties.getProperty("persistence.tree.cutoff.major");
    bfsChunkSize = properties.getProperty("persistence.bfs.chunk.max", 50000);
    setTopNMultiplier(properties, true);

    dynamicProperties = (DynamicPropertiesUtil)properties;
    dynamicProperties.addListener(new DynamicPropertiesUtil.Listener() {

        public void propertiesChanged(DynamicPropertiesUtil properties) {
          allowCompression = properties.getProperty("persistence.allow.compression", true);
          batchAppendTrees = properties.getProperty("persistence.batch.append.trees", BATCH_APPEND_TREES_DEFAULT);
          setTopNMultiplier(properties, false);
          localeIndependentLatency = properties.getProperty("persistence.enable.locale.independent.latency", false);
        }



        public void propertiesScanned(DynamicPropertiesUtil properties) {
          // Do nothing
        }
      }, 60);


    //why 50000? Because it seems like a reasonable amount of memory to use. Since
    //this determines the amount of NodeHolders we write to summary file before
    //writing trends, the larger the better, except for the fact that memory is
    //finite.
    recordChunk = properties.getProperty("persistency.record_chunk", 50000);

    //56 seems like a good number of queries to cache since the UI displays (by default)
    //7 entities, we want to be able to cache a "full screen", which is 7 entities * (7
    //trended fields + 1 summary) = 56
    int maxCacheSize = properties.getProperty("query.max.cache.size", 56);

    queryResultCache.setMaxSize(maxCacheSize);

    super.initialize(module);
  }



  /**
   * Persistency Module Object initialization
   * Configures persistency durations, starts reaggregation thread
   *
   * @param module
   */
  public void postInitialize(IModule module) {

    PropertiesUtil properties = module.getModuleProperties();

    timeManager = (ServerTimeManager)module.getObjectByType(ServerTimeManager.class);
    symbolManager = (ISymbolManager)module.getObjectByType(ISymbolManager.class);
    storageManager = (StorageManager)module.getObjectByType(StorageManager.class);
    statusNodeFactory = (IStatusNodeFactory)module.getObjectByType(IStatusNodeFactory.class);
    instanceTreeSelector = (InstanceTreeSelector)getModule().getObjectByType(InstanceTreeSelector.class);

    reaggregationManager = new ReaggregationManager(storageManager, this,
      properties, queryLock, timeManager, compressionBlocker);
    nodeHolderComparator = new PathComparator(symbolManager);

    MonitorModule monitorModule = (MonitorModule)module.getModuleLoader().getModule(MonitorModule.class);

    if (null != monitorModule) {
      IMonitor monitor = monitorModule.getMonitor();

      if (null != monitor) {
        metrics.register(monitor);
      }
    }

    // Create a single thread that when executing one request will block until that is complete.
    // Important to not exceed the number of threads, since we rely on this to block asynchBucketComplete, and therefore the onlineBucketComplete
    // for a second call.
    PooledExecutor tmpBucketCompleteExecutor = new PooledExecutor(1);

    tmpBucketCompleteExecutor.waitWhenBlocked();
    // Use a specific ThreadFactory in order to get a meaningful name for the thread
    tmpBucketCompleteExecutor.setThreadFactory(new ThreadFactory() {

      public Thread newThread(Runnable command) {
        return new Thread(command, "Persist Online Bucket");
      }
    }
    );
    bucketCompleteExecutor = tmpBucketCompleteExecutor;

    MajorDuration[] majors = storageManager.getMajorDurations();

    TrendFile.initializeTransposeBufferCache(getMaxRecordLength(majors));
    initializeReagregation(majors);

    super.postInitialize(module);
  }



  private void initializeReagregation(MajorDuration[] majors) {
    // todo: MV: this starts immediately on startup, do we want to wait for system to fully startup before loading it with aggregation?
    reagEventScheduler = InfrequentEventScheduler.createDedicatedInstance("Persistency Reaggregation", Thread.NORM_PRIORITY);

    int smallestMinorDuration = (int)(majors[0].getMinorDurations()[0].getDuration(System.currentTimeMillis()) / 1000);

    reagEventScheduler.addEvent(reaggregationManager, smallestMinorDuration);

    Loggers.ARCHIVE.debug("Set reaggregation to " + smallestMinorDuration);
  }



  /** update the value of topN multiplier */
  private void setTopNMultiplier(PropertiesUtil properties, boolean first) {
    double value = properties.getProperty("persistence.topn.multiplier", 1.1);

    if (value < 1.0) {
      if (!first) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
          "invalid topn multiplier", "valid topn multiplier", Long.MAX_VALUE).warning("invalid topN multiplier (" + value +
          "), will use default of 1.1 instead. This message logged once until valid value is set.");
      }
      value = 1.1;
    }
    else if (value > 2.0) {
      if (!first) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
          "invalid topn multiplier", "valid topn multiplier", Long.MAX_VALUE).warning("invalid topN multiplier (" + value +
          "), will use default of 1.1 instead. This message logged once until valid value is set.");
      }
      value = 1.1;
    }
    else {
      if (!first) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
          "valid topn multiplier", "invalid topn multiplier", Long.MAX_VALUE).info("persistence topN multiplier set to " + value);
      }
    }

    topNMultiplier = value;

  }



  /** get the maximum number of points a trend can have */
  private int getMaxRecordLength(MajorDuration[] majors) {
    int maxRecordLength = 0;

    for (MajorDuration major: majors) {
      int majorRecordLength = major.getMaxTrendPoints(majors);

      if (majorRecordLength > maxRecordLength) {
        maxRecordLength = majorRecordLength;
      }
    }
    return maxRecordLength;
  }



  /**
   * Persistence resource manager
   */
  IResourceManager getResourceManager() {
    return resourceManager;
  }



  /**
   * Return an instance of status node factory.
   */
  IStatusNodeFactory getStatusNodeFactory() {
    return this.statusNodeFactory;
  }



  /**
   * Entry point into persistence for buckets as they leave OnlineCache.
   * <p/>
   *
   * @param bucketTime the bucket time the records are for
   * @param granularityData the granularity template the records are for
   * @param groupByRecords the record contexts for each group by to scan and create NodeHolders for
   */
  @InstrumentationPoint(layer = "Persistence")
  public void onlineBucketComplete(final long bucketTime, final IGranularityData granularityData, Map<IAggregateTreeNode, IBucketedAggregateTreeNode.BucketedRecordContext> groupByRecords) {
    if (disableOnlineDataPersistence) {
      Loggers.ARCHIVE.info("Online data persistence is disabled, bucket of online data was discarded");
      return;
    }

    if (groupByRecords == null || groupByRecords.size() == 0) {
      return;
    }

    synchronized (obcLock) {
      if (permanentSevereCondition) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE, "Pathed keys force failed, look for disk failure or full disk condition", "No reset for this one", 1000 * 60).severe("Pathed keys force failed, look for disk failure or full disk condition");
        throw new IllegalStateException("Force failure for pathed keys");
      }

      //We MUST force the path keys to the symbol file since any keys that have been created
      //will now be referenced from the data store, so those keys need to exist. The only
      //wayt to do this is to force a flush of them.
      try {
        symbolManager.forcePathKeys();
      }
      catch (Throwable e) {
        permanentSevereCondition = true;
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE, "Pathed keys force failed, look for disk failure or full disk condition", "No reset for this one", 1000 * 60).severe("Pathed keys force failed, look for disk failure or full disk condition");
        throw new IllegalStateException("Force failure for pathed keys", e);
      }

      //A map containing the result of createNodeHolders for each of the group by's
      final Map<GroupBy, TreeSet<NodeHolder>> nodeHolders = new HashMap<GroupBy, TreeSet<NodeHolder>>();

      for (Map.Entry<IAggregateTreeNode, IBucketedAggregateTreeNode.BucketedRecordContext> groupByEntry : groupByRecords.entrySet()) {
        if (Loggers.ARCHIVE.isEnabledFor(Level.DEBUG)) {
          Loggers.ARCHIVE.debug("received bucket for archival: " + groupByEntry.getValue().getGranularity() +
            " bucket: " + groupByEntry.getValue().getBucketRecordIndex());
        }

        IAggregateTreeNode groupBy = groupByEntry.getKey();

        if (!groupBy.getChildIterator().hasNext()) {
          // If groupby has no children then don't create node holders for it.
          continue;
        }

        GroupBy groupByData = (GroupBy)groupBy.getData();

        if (nodeHolders.containsKey(groupByData)) {  // This relies on equality of GroupBy to account for both customer and run names, be carefull than changing GroupBy.equals
          throw new IllegalStateException("NodeHolders already contain thigroupByEntryBy");
        }

        TreeSet<NodeHolder> groupByHolders = new TreeSet<NodeHolder>(new PathComparator(symbolManager));

        createNodeHolders(groupByHolders, groupBy, groupByEntry.getValue().getGranularity(), groupByEntry.getValue().getBucketRecordIndex());

        if (groupByHolders.size() < 2) { // Since we separated groupBys, each groupByHolders that has only 1 object, has only the group by in it, which we don't want to persist.
          continue; // empty group by's are not interesting
        }
        nodeHolders.put(groupByData, groupByHolders);
      }

      if (nodeHolders.size() == 0) {
        // If no node holders created, then don't bother persisting
        return;
      }

      try {
        this.bucketCompleteExecutor.execute(new Runnable() {
          public void run() {

          	if (bucketWriteDelay > 0) { // for SaaS: throttle to avoid too many concurrent disk writes at the same time
          		try {
        	      Thread.sleep(bucketWriteDelay);
              } catch (InterruptedException e) { }
          	}
          	
            try {
              compressionBlocker.beginBlock();
              reaggregationBlocker.beginBlock();
              asynchBucketCompleteAggregation(nodeHolders, bucketTime, granularityData);
            }
            catch (Exception e) {
              Loggers.ARCHIVE.severe("Online bucket persistence failed, some data loss have occured " + e, e);
            }
            finally {
              if (compressionBlocker != null) {
                compressionBlocker.endBlock();
              }
              reaggregationBlocker.endBlock();
            }
          }
        }
        );
      }
      catch (InterruptedException e) {
        // Was interrupted to stop
      }
    }
  }



  /**
   * Aggregate the nodeHolders into persistence. This is the asynchronous
   * (OnlineCache is not locked) side of the OC bucket complete aggregation.
   */
  @InstrumentationPoint(layer = "Persistence")
  private synchronized void asynchBucketCompleteAggregation(Map<GroupBy, TreeSet<NodeHolder>> groupHolders,
    long startTime, IGranularityData template) {
	
    for (GroupBy groupBy : groupHolders.keySet()) {

      if (!purger.startAggregating(groupBy)) {
        Loggers.ARCHIVE.debug("Skipping data for group " +
          groupBy.getDisplayName() + " that is scheduled for deletion");
        continue;
      }

      PersistenceAggregationContext context = new PersistenceAggregationContext();

      TreeSet<NodeHolder> nodeHolders = groupHolders.get(groupBy);

      try {
        context.startTime = startTime;
        context.major = storageManager.getMajorDurations()[0];
        context.minor = template;
        long majorResolution = context.major.getTrendResolution(storageManager.getMajorDurations(), startTime);
        long minorResolution = template.getTrendResolution();

        //this consolidation is to handle the case where OC and first granularity of persistence
        //do not have the same point size.
        if (majorResolution % minorResolution == 0 &&
          majorResolution >= minorResolution) {
          context.consolidateRatio = (int)(majorResolution / minorResolution);
        }
        else {
          throw new IllegalStateException("Online bucket resolution must be <= first persistence major resolution and must be a multiple");
        }

        context.mediaMap = null;
        context.doingReaggregation = false;
        context.destinationMedium = storageManager.getTreeStorageMedium(groupBy,
          context.major, context.startTime, IStorageManager.AccessMode.WRITE,
          IStorageManager.LockMode.BLOCKING_EXCLUSIVE);

        storeInstanceTrees(nodeHolders, context, batchAppendTrees);
        updateMinors(groupBy, context, nodeHolders, null, null);
        reaggregationManager.updateRotation(groupBy, context.writersToRotate);
      }
      catch (IOException e) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
          "IO Exception occured when processing online bucket granularity " + template, null,
          Long.MAX_VALUE).severe("IO Exception occured when processing online bucket for granularity " +
          template + ", data will not be persisted " + e, e);
      }
      catch (DurationNotFoundException e) {
        InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
          "Major duration not found for granularity " + template, null,
          Long.MAX_VALUE).warning("Major duration not found for granularity " +
          template + ", data will not be persisted " + e);
      }
      finally {
        purger.clearGroupByBeingCreated();

        if (context.destinationMedium != null) {
          if (!context.destinationMedium.isClosed()) {
            try {
              context.destinationMedium.close(false);
            }
            catch (IOException e) {
              Loggers.ARCHIVE.warning("Attempt to close destination mediumn failed");
            }
          }
        }
      }
    }

  }



  /**
   * Stores instance trees from online bucket in FileStorageMedium
   * @param nodeHolders
   * @param context
   * @throws java.io.IOException
   */
  @InstrumentationPoint(layer = "Persistence")
  private void storeInstanceTrees(TreeSet<NodeHolder> nodeHolders,
    PersistenceAggregationContext context, boolean batchAppendTrees ) throws IOException {

    NodeHolder.storeInstanceTrees(nodeHolders, context.destinationMedium, symbolManager,
            instanceTreeSelector.getInstanceStorageManager(), metrics.treeNodesStored, batchAppendTrees);
  }



  /**
   * Writes trends to positions indicated by recordmap
   *
   * @param recordMap    - A mapping of previously existing trend positions
   * @param trendFile    - The trend file to which to write the trends
   * @param recordOffset - An offset into the record in the file calculated based on time offset vs the major
   *                     todo: getting the actual records is wrong. Should implement the getRawMetrics etc..
   */
  @InstrumentationPoint(layer = "Persistence")
  private void writeTrends(TreeSet<TrendMapper> recordMap, TrendFile trendFile,
    int recordOffset, PersistenceAggregationContext context, WritableDurationSummaryIterator writer)
    throws IOException {

    double[] values = null;

    for (TrendMapper mapper: recordMap) {

      // Skip writing of empty trends if node holder tells us so. It will (currently) do so
      // when field's value is 0 and the field uses sum-based aggregation, thus its trend must
      // be an empty one (all 0's).
      if (mapper.holder.mustHaveEmptyTrend(mapper.field)) {
        continue;
      }

      if (context.doingReaggregation) {

        //give preference to the online aggregation (which has a much harder time limit in which it has to do its thing)
        waitForAsynchBucketComplete();

        //when reaggregating from persistence the summary for the source is a
        //single data point in the destination, so create a 1 value array with
        //the summary value
        if (null == values || values.length != 1) {
          values = new double[1];
        }
        values[0] = mapper.holder.getFieldValue(mapper.field);
      }
      else {
        if (mapper.holder.getRecord() != null) {
          values = (mapper.holder.getRecord()).getPersistedTrend(mapper.field); //TODO: The implementation of getPersistedTrend is not very efficient, can be made better
          if (context.consolidateRatio > 1) {
            // consolidation is here to support the case where online cache granularity is lower than first persistence minor's granularity
            values = consolidateTrend(values.length / context.consolidateRatio, mapper.field, values);
          }
        }
      }

      if (values != null) {
        if (mapper.trendPosition < -1) {
          Loggers.ARCHIVE.warning("Negative trend position of " + mapper.trendPosition + " when aggregating " + context.minor.getName() + " granularity at start time " + context.startTime + " to file " + trendFile.toString() + " , skipping trend value ");
          continue;
        }
        else if (mapper.trendPosition < TrendFile.HEADER_SIZE && mapper.trendPosition != -1) {
          Loggers.ARCHIVE.warning("Trend position of  " + mapper.trendPosition + " is less than TrendFile.HEADER_SIZE when aggregating " + context.minor.getName() + " granularity at start time " + context.startTime + " to file " + trendFile.toString() + " , skipping trend value ");
          continue;
        }

        try {
          long trendPosition = trendFile.write(mapper.trendPosition, values, recordOffset);

          mapper.holder.setTrendPosition(trendPosition, mapper.field);
          mapper.trendPosition = trendPosition;
          mapper.written = true;

          if (writer.getLastTrendOffsetPosition() < values.length + recordOffset) {
            writer.updateLastTrendOffsetPosition(recordOffset + values.length);
            // This helps us keep track of the last updated made to a given major
            context.major.setCurrentTrendOffset(recordOffset + values.length);
          }
        }
        catch (IllegalArgumentException e) {
          Loggers.ARCHIVE.warning("Illegal argument when writing trend, value skipped " + e, e);
        }
      }
    }
  }



  /**
   * Same as updateMinorDurations for onlineCache, this one is for persisted
   * summaries. Called by reaggregation manager is given an iterator into a
   * summary and will setup and call the aggregation of that summary into all
   * minors of a given major
   *
   * @param durationToReaggregate Granularity of the source data
   * @param originalDur A reader for the source data (eg, the 5 minute summary file)
   * @param startTime The time that the source data (originalDur) starts at
   * @param majorToReaggregate The id of the major we're reaggregating into
   * @param upgradingToTotal  is this aggregation happening to populate the
   *                           total summary (if so, minors won't be aggregated)
   * @param minorsToReaggregate The minors that need reaggregation. This may be
   *        null, in which case ONLY total aggregation will be done
   * @throws com.mercury.diagnostics.server.persistence.impl.DurationNotFoundException
   * @throws java.io.IOException
   */
  @InstrumentationPoint(layer = "Persistence")
  public PersistenceAggregationContext aggregateDurations(GroupBy groupBy,
    IGranularityData durationToReaggregate, DurationSummaryIterator originalDur,
    long startTime, MajorDuration majorToReaggregate,
    boolean upgradingToTotal, List<MajorMinor> minorsToReaggregate)
    throws DurationNotFoundException, IOException {

    PersistenceAggregationContext context = new PersistenceAggregationContext();

    context.major = majorToReaggregate;
    context.destinationMedium = storageManager.getTreeStorageMedium(groupBy,
      context.major, startTime, IStorageManager.AccessMode.WRITE,
      IStorageManager.LockMode.BLOCKING_EXCLUSIVE);
    context.minor = durationToReaggregate;
    context.startTime = startTime;
    context.mediaMap = new HashMap<Long, Long>();
    context.doingReaggregation = true;

    if (!upgradingToTotal && null == minorsToReaggregate) {
      throw new IllegalArgumentException("you must specify the minors unless upgrading to total");
    }

    try {
      // If the major tracks a total summary, aggregate it before the rest of the summaries
      if (majorToReaggregate.hasTotalSummary()) {

        long purgingTimeStamp;

        purgingTimeStamp = majorToReaggregate.getTotalDurationInMillis(
          timeManager.getCurrentTargetTime());
        updateTotal(groupBy, context, originalDur, purgingTimeStamp, upgradingToTotal);
      }
      if (!upgradingToTotal) {
        updateMinors(groupBy, context, null, originalDur, minorsToReaggregate);
      }
    }
    catch (IOException e) {
      InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
        "ReaggregateContextError-" + context.major + context.minor,
        null, 1000 * 60 * 5).warning("Exception during reaggregation of context: " +
          context.toString() + ", " + e, e);
      
      throw e;
    }
    catch (RuntimeException e) {
      InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
        "ReaggregateContextError-" + context.major + context.minor,
        null, 1000 * 60 * 5).warning("Exception during reaggregation of context: " +
          context.toString() + ", " + e, e);
      
      throw e;
    }
    finally {
      if (context.destinationMedium != null) {
        if (!context.destinationMedium.isClosed()) {
          try {
            context.destinationMedium.close(false);
          }
          catch (IOException e) {
            Loggers.ARCHIVE.warning("Attempt to close destination mediumn failed");
          }
        }
      }
    }

    return context;
  }



  /**
   * Aggregates the source into the total summary duration,
   * and deletes path keys based on last modified field in the total summary duration
   *
   * This must happen BEFORE any other aggregations. Failing to do this will cause data to
   * be counted incorrectly since the marks will indicate that it has already been aggregated.
   *
   * very similar to updateMinor()....is it possible to refactor them into a single method?
   * @param groupBy
   * @param context
   * @param offlineNodeHolders
   * @param purgingTimeStamp
   * @param upgradingToTotal
   * @throws com.mercury.diagnostics.server.persistence.impl.DurationNotFoundException
   */
  private void updateTotal(GroupBy groupBy,
    PersistenceAggregationContext context,
    DurationSummaryIterator offlineNodeHolders,
    long purgingTimeStamp, boolean upgradingToTotal)
    throws DurationNotFoundException {

    DurationSummaryIterator durationFileIter = null;
    WritableDurationSummaryIterator durationWriter = null;

    boolean writerComplete = false;

    try {
      //  Summaries aren't overwritten - we read the previous version (durationFileIter),
      //  updating it as we read, then write the new version (durationWriter) with a
      //  .write extension.  When we're done, we atomically "rotate" (delete the original,
      //  rename the new without the .write extension).
      File summaryFile = storageManager.getTotalSummaryFile(groupBy, context.major);

      durationFileIter = storageManager.getDurationSummaryIterator(summaryFile,
        IStorageManager.LockMode.BLOCKING_SHARED);

      //set the first bit to indicate that the upgrade from yearly to total
      //summary has happened.
      BitSet aggregatedBuckets = new BitSet();

      if (upgradingToTotal) {
        aggregatedBuckets.set(0);
      }

      durationWriter = storageManager.getTotalWritableDurationSummaryIterator(
        groupBy, context.major, aggregatedBuckets);

      Loggers.ARCHIVE.debug("Reaggregating " + durationFileIter.file);

      Iterator<NodeHolder> nodeIter = null;

      offlineNodeHolders.reset();
      nodeIter = offlineNodeHolders;

      int offset = -1;

      // wait for asynchbucketcomplete, since it has higher priority
      //
      waitForAsynchBucketComplete();

      List<NodePositions> parentPositions = aggregateIteratorToFile(
        nodeIter, durationFileIter, durationWriter, null, offset, null,
        context, true, purgingTimeStamp);

      waitForAsynchBucketComplete();

      updateChildPositions(durationWriter, parentPositions);

      //LH - don't close the iterators, they will be closed when rotation happens rotated
      //MV - only rotate if no error occured. We don't want bad data to be rotated
      //PM - ?? Doesn't this hold multi-megabyte buffers open until they're closed?
      //     Moshe agrees that we should just pass the file names to writersToRotate and
      //     close the files here.
      //LH - sounds reasonable to me....
      context.writersToRotate.put(durationWriter, durationFileIter);
      writerComplete = true;
    }
    catch (IOException e) {
      Loggers.ARCHIVE.severe("Could not initialize summary iterator: " + e, e);
      return; //don't want to continue, the error is likely not going to be resolved
    }
    finally {
      // Writer will not be in writers to rotate if something went wrong,
      // close the writer in this case, so we don't keep the lock
      if (!writerComplete) {
        if (durationWriter != null) {
          try {
            durationWriter.truncate();
            durationWriter.forceClose();
          }
          catch (Exception e) {
            Loggers.ARCHIVE.warning("Closing duration writer failed " + e, e);
          }
        }
        if (durationFileIter != null) {
          try {
            durationFileIter.close();
          }
          catch (Exception e) {
            Loggers.ARCHIVE.warning("Closing duration reader failed " + e, e);
          }
        }
      }
    }
  }



  /**
   * After the trends are written, the actual positions of the trends written are written
   * to the corresponding nodeholders, and these are used to update the summary files with trend positions
   *
   * @param durationWriter
   * @param mappers
   */
  private void updateTrendPositionsForMajor(
    WritableDurationSummaryIterator durationWriter, TreeSet<TrendMapper> mappers) throws IOException {

    Set<Long> done = new HashSet<Long>();

    //mappers is in summary file order, so we won't be thrashing around doing this iteration.
    for (TrendMapper mapper : mappers) {
      if (mapper.written) {
        if (!(done.contains(mapper.holder.getPathedKey()))) {
          durationWriter.updateTrendPositions(mapper.holder);
        }
        done.add(mapper.holder.getPathedKey());
      }
    }

    durationWriter.flushAppends();
  }



  /**
   * Given a set of nodeholders, preordered in the order in which the summary
   * files are ordered, iterate through all minors in a major and ask to
   * reaggregate the nodeholders into those minors
   *
   * @param groupBy Which groupby the minors belong to
   * @param nodeHolders data to be aggregated into the minors when it comes from the online cache -- null otherwise
   * @param offlineNodeHolders data to be aggregated into the minors when it comes from another summary -- null otherwise
   * @param minorsToReaggregate the minor durations the reaggregation should go into (may be null to indicate all minors in the major)
   *
   */
  @InstrumentationPoint(layer = "Persistence")
  private void updateMinors(GroupBy groupBy,
    PersistenceAggregationContext context, Set<NodeHolder> nodeHolders,
    DurationSummaryIterator offlineNodeHolders,
    List<MajorMinor> minorsToReaggregate)
    throws DurationNotFoundException, IOException {

    if (null != nodeHolders && null != offlineNodeHolders) {
      throw new IllegalArgumentException("only one of nodeHolders and offlineNodeHolders can be specified");
    }

    DurationSummaryIterator durationFileIter = null;
    WritableDurationSummaryIterator durationWriter = null;
    TreeSet<TrendMapper> recordMap;
    TrendFile trendFile = null;

    context.originalMedium = null;

    try {
      if (context.doingReaggregation) {
        MajorDuration originalMajor = storageManager.getMajorForGranularity(context.minor);

        context.originalMedium = storageManager.getTreeStorageMedium(groupBy,
          originalMajor, context.startTime, IStorageManager.AccessMode.READ,
          IStorageManager.LockMode.BLOCKING_EXCLUSIVE);
      }
      else {
        context.originalMedium = context.destinationMedium;
      }

      if (null == minorsToReaggregate) {
        minorsToReaggregate = new ArrayList<MajorMinor>();
        for (IGranularityData minor: context.major.getMinorDurations()) {
          minorsToReaggregate.add(new MajorMinor(context.major, minor));
        }
      }

      //aggregate into all of the minors in the major
      for (MajorMinor destination: minorsToReaggregate) {
        IGranularityData minor = destination.minor;
        boolean writerComplete = false;

        durationFileIter = null;
        durationWriter = null;
        trendFile = null;

        try {

          long minorStartTime = minor.getDurationStartTime(context.startTime);

          // Since we are updating a certain bucket in a given minor duraion
          // we can use this place to indicate what current bucket is
          minor.setLastModifiedBucketStartTime(minorStartTime);

          //  Summaries aren't overwritten - we read the previous version (durationFileIter),
          //  updating it as we read, then write the new version (durationWriter) with a
          //  .write extension.  When we're done, we atomically "rotate" (delete the original,
          //  rename the new without the .write extension).
          File summaryFile = storageManager.getSummaryFile(groupBy,
            context.major, minor, context.startTime);

          //  Don't ever reaggregate INTO a compressed summary.  Compressed summaries are
          //  definitely complete.
          if (!(IStorageManager.Suffix.COMPRESSED.ends(summaryFile.getName()))) {

            durationFileIter = storageManager.getDurationSummaryIterator(
              summaryFile, IStorageManager.LockMode.BLOCKING_SHARED);

            BitSet buckets = new BitSet();

            buckets.or(durationFileIter.getAggregatedBuckets());
            buckets.set(Helper.getSourceBucketIndex(context.startTime, context.minor, minor));

            durationWriter = storageManager.getWritableDurationSummaryIterator(groupBy,
              context.major, minor, context.startTime, buckets);

            Loggers.ARCHIVE.debug("Reaggregating " + durationFileIter.file);

            Iterator<NodeHolder> nodeIter = null;

            if (nodeHolders != null) {
              nodeIter = nodeHolders.iterator();
            }
            else if (offlineNodeHolders != null) {
              offlineNodeHolders.reset();
              nodeIter = offlineNodeHolders;
            }

            int offset = -1;

            if (minor.getIndex() == context.major.getMinorDurations().length - 1) {
              //  This is the major minor, so we do trends too
              recordMap = new TreeSet<TrendMapper>();
              offset = StorageManager.getDurationTrendOffset(
                context.minor, context.major, context.startTime, storageManager.getMajorDurations());
              trendFile = storageManager.getTrendFile(groupBy,
                context.major, context.startTime,
                IStorageManager.AccessMode.WRITE, IStorageManager.LockMode.BLOCKING_EXCLUSIVE);
            }
            else {
              recordMap = null;
            }

            // check if we are in offline reaggregation,
            // if this is the case, wait for asynchbucketcomplete, since it has higher priority
            //
            if (context.doingReaggregation) {
              waitForAsynchBucketComplete();
            }

            List<NodePositions> parentPositions = aggregateIteratorToFile(
              nodeIter, durationFileIter, durationWriter, recordMap, offset,
              trendFile, context, false, 0);

            if (context.doingReaggregation) {
              waitForAsynchBucketComplete();
            }

            updateChildPositions(durationWriter, parentPositions);

            //LH - don't close the iterators, they will be closed when rotation happens rotated
            //MV - only rotate if no error occured. We don't want bad data to be rotated
            //PM - ?? Doesn't this hold multi-megabyte buffers open until they're closed?
            //     Moshe agrees that we should just pass the file names to writersToRotate and
            //     close the files here.
            //LH - sounds reasonable to me...
            //LH - at one point when file locking actually sort of worked, we
            //     would keep the iterators open so they would keep the files
            //     locked to increase the chances of not failing to rotate.
            context.writersToRotate.put(durationWriter, durationFileIter);
            writerComplete = true;

          }
        }
        catch (IOException e) {
          Loggers.ARCHIVE.severe("Could not initialize summary iterator: " + e, e);
          return; //don't want to continue, the error is likely not going to be resolved
        }
        finally {
          try {
            if (trendFile != null) {
              trendFile.close();
              //  TODO: I think we need to set trendFile = null here to be safe
            }
          }
          finally {
            // Writer will not be in writers to rotate if something went wrong,
            // close the writer in this case, so we don't keep the lock
            if (!writerComplete) {
              if (durationWriter != null) {
                try {
                  durationWriter.truncate();
                  durationWriter.forceClose();
                }
                catch (Exception e) {
                  Loggers.ARCHIVE.warning("Closing duration writer failed " + e, e);
                }
              }
              if (durationFileIter != null) {
                try {
                  durationFileIter.close();
                }
                catch (Exception e) {
                  Loggers.ARCHIVE.warning("Closing duration reader failed " + e, e);
                }
              }
            }
          }
        }
      }
    }
    finally {
      // todo:  - make all those finally better - UGH - ugly
      if (context.originalMedium != null) {
        if (!context.originalMedium.isClosed()) {
          try {
            context.originalMedium.close(false);
          }
          catch (IOException e) {
            Loggers.ARCHIVE.warning("Attempt to close original mediumn failed");
          }
        }
      }
    }
  }



  public void waitForAsynchBucketComplete() {
    try {
      reaggregationBlocker.waitForNoBlocks();
    }
    catch (InterruptedException e) {
      Loggers.ARCHIVE.warning("Waiting for asynchBucketComplete failed due to " + e);
    }
  }



  /**
   * Traverse the aggregate tree and build NodeHolder Set ordered with PathedComparator.
   * This runs with OnlineCache locked since we are traversing the cache. Upon return, we
   * must have all the data we need to process the bucket WITHOUT doing any traversals. Since
   * OC will take control of the tree at that point. We keep references to the records, which
   * is OK since that bucket is complete and OC MUST NOT make any changes to the records after
   * passing the bucket to us. Since the next onlineBucketComplete() call blocks until this
   * one is complete, we don't need to worry about the records being reused while we are stil
   * using them.
   *
   * @param rootNode
   * @param granularity
   * @param bucketIndex
   * @criticalpath
   */
  @InstrumentationPoint(layer = "Persistence")
  private void createNodeHolders(final TreeSet<NodeHolder> nodeHolders, ITreeNode rootNode, final IGranularity granularity, final int bucketIndex) {

    class DLevel extends ITreeTraverser.Level {
      NodeHolder parentHolder;

      public DLevel(ITreeNode node, ITreeTraverser.Level parent, NodeHolder parentHolder) {
        super(node, parent);
        this.parentHolder = parentHolder;
      }
    }

    new TreeTraversal(rootNode,
      new ITreeTraverser() {

      public Level enterNode(ITreeNode node, Level lvl) {
        long pathedKey;
        INodeData data = node.getData();

        //todo: can not skip, otherwise i have nodes with null parents!
        if (shouldSkip(data, lvl)) {
          return ITreeTraverser.SKIP;
        }

        if (data.getNodeIdentifier().getSymbolToken() == 0) {
          InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
            "A NodeData " + data + " of type " + data.getClass().getName() + " has unassigned symbol key", null,
            120000).severe("A NodeData " + data + " of type " + data.getClass().getName() + " has unassigned symbol key");
          int symbolKey = symbolManager.getLocalKey(data, true);

          if (symbolKey == 0) {
            Loggers.ARCHIVE.severe("Assignment of symbolKey to data failed, data was " + data);
            throw new IllegalArgumentException("Assignment of symbolKey to data failed, data was " + data);
          }
          data.getNodeIdentifier().setSymbolToken(symbolKey);
        }

        pathedKey = node.getPathKey();

        NodeHolder parentHolder = lvl == null ? null : ((DLevel)lvl).parentHolder;

        if (pathedKey == NodeDataFactory.UNSPECIFIED_PATH_KEY) {
          InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
            "Pathed key from INode was not assigned, reassigning", null,
            120000).severe("Pathed key from INode was not assigned, reassigning");
          if (lvl == null) {
            pathedKey = symbolManager.assignKey(0L, data.getNodeIdentifier().getSymbolToken());
          }
          else {
            long parentPathKey = parentHolder == null ? NodeDataFactory.UNSPECIFIED_PATH_KEY : parentHolder.getPathedKey();

            pathedKey = symbolManager.assignKey(parentPathKey, data.getNodeIdentifier().getSymbolToken());
          }
        }

        PersistedRecord record = null;

        if (node instanceof BucketedAggregateTreeNode) {
          IRecord iRecord = ((BucketedAggregateTreeNode)node).getRecord(granularity, bucketIndex);

          if (iRecord instanceof PersistedRecord) {
            record = (PersistedRecord)iRecord;
          }
        }

        if (data instanceof MetricData && record == null) {
          return ITreeTraverser.SKIP;
        }

        INode latencyNode = ((IAggregateTreeNode)node).getChildNode(MetricData.LATENCY_METRIC);
        if (latencyNode == null && data.shouldHaveLatencyNode()) {
          latencyNode = localeIndependentLatency(node, MetricData.LATENCY_METRIC);
        }

        if (latencyNode != null) {
          IRecord iRecord = ((BucketedAggregateTreeNode)latencyNode).getRecord(granularity, bucketIndex);

          if (iRecord == null) {
            return ITreeTraverser.SKIP;
          }

          if (iRecord instanceof PersistedRecord) {
            record = (PersistedRecord)iRecord;
            if (record.getPersistedFieldValue(PersistedRecord.COUNT_FIELD) <= 0) {

              if (Loggers.ARCHIVE.isEnabledFor(com.mercury.diagnostics.common.logging.Level.DEBUG)) {
                InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
                  "zero count error", null, 1000 * 60 * 5).severe("Zero count in " + node.getData().getDisplayName() + "/" + latencyNode.getData().getDisplayName() + " record is " + iRecord.getClass().getName(),
                  new IllegalStateException("Count on fragment's latency can not be zero"));
              }

              return ITreeTraverser.SKIP; // no big deal, double bucketing + late data, means we created a record but didn't have anything to put there.

            }
          }
        }

        warnOnMissingRecord(record, data);

        List<StoredTreeRecord> treeRecords = getInstanceTreeRecords(node, granularity, bucketIndex);
        NodeHolder holder = new NodeHolder(record, pathedKey, treeRecords,
          ((data instanceof FragmentData || data instanceof MethodData || data instanceof ServiceData) ? instanceTreeSelector : null), WritableReader.getId(data));

        holder.setLastModified(timeManager.getCurrentTargetTime());
        nodeHolders.add(holder);
        if (parentHolder != null) {
          parentHolder.setChildrenTypes(parentHolder.getChildrenTypes() | data.getNodeDataType());
        }
        return new DLevel(node, lvl, holder);

      }

        /**
         * If localeIndependentLatency is enabled, try to find latency by name.
         *
         * When we don't find the expected latency, it might be due to a locale problem (QC 33920).
         * We do uppercase matching on units, but microseconds uppercased in Turkish doesn't match
         * microseconds uppercased in English.  So if the probe/server/client don't all have that fixed
         * then we can run into missing latency.  Since this might keep happening until all probes are
         * 8.03 or later, we'll leave the workaround in.  But this may be a bit performance hoggy, so it
         * is disabled by default.
         *
         * @param node current node
         * @param expected This is the MetricData.LATENCY that we actually looked for (used for logging).
         * @return The latency node that we found, or null.
         */
        private INode localeIndependentLatency(ITreeNode node, MetricData expected) {
          if (localeIndependentLatency) {
            Iterator<INode> nodeIter = node.getChildIterator(MetricData.class);
            while (nodeIter.hasNext()) {
              INode inode = nodeIter.next();
              MetricData mData = (MetricData)inode.getData();
              if (mData.getName().equals(Metric.LATENCY)){
                logSevereMissingLatencyWorkaround(expected, mData);
                return inode;  // found it, use it, bail out now.
              }
            }
          }

          return null;
        }


      private boolean shouldSkip(INodeData data, Level parent) {
        if (data instanceof TreeData) {
          return true;
        }

        if (data instanceof MetricData && MetricData.isLatencyMetric(data)) {
          // Metrics are skipped; directly handled by the parent NodeHolder.
          return true;
        }

        if (data instanceof MethodData) {
          if (parent.getNode().getData() instanceof FragmentData) {
            // If this is a MethodData below a FragmentData, then skip, this
            // represents a full aggregate tree that we only need in the
            // OnlineCache.
            return true;
          }

          // MethodData can be elsewhere in the tree (trended_method), so don't
          // skip those.
        }

        return false;
      }



      /**
       * Warn if we we expect to have a record, but don't.
       *
       * Note that this is only a warning, we will still create and persist
       * the nodeholder (which may cause problems later on). The reason for
       * doing this is if we don't persist this node, we won't be able to find
       * any of its children nodes, and I'd rather localize the data loss
       * as best we can.
       */
      private void warnOnMissingRecord(PersistedRecord record, INodeData data) {
        if (null == record && shouldHaveRecord(data)) {
          NodeIdentifier identifier = data.getNodeIdentifier();
          StringBuffer buf = new StringBuffer(data.getClass().getName());

          buf.append(": ");
          if (null == identifier) {
            buf.append(data.toString());
          }
          else {
            buf.append(identifier.toString());
          }

          String name = buf.toString();

          InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
            "null record on " + name, null, InfrequentLogger.DAILY ).warning("persistence of entity with no record: " + name);
        }
      }


        /**
         * Log warning when the missing latency work-around is actually used.
         * When enabled, this log message will hopefuly show us exactly what the problem was and convince everyone
         * that patches are needed somewhere because this is supposedly fixed in 8.03.
         */
        private void logSevereMissingLatencyWorkaround(INodeData expectedData, INodeData foundData) {
          NodeIdentifier expectedID = expectedData.getNodeIdentifier();
          NodeIdentifier foundID = foundData.getNodeIdentifier();
          InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE,
                  "found latency as " + foundID, null, 1000 * 60 * 60).severe("Looking for latency as " + expectedID
                  + " but had to use workaround to find this instead: " + foundID);
        }

      /**
       * Should nodes with the given data have a latency record?
       *
       * In general, yes. However, GroupBy and ProbeGroupData do not have
       * data rolled up to them, so they won't ever have records.
       */
      private boolean shouldHaveRecord(INodeData data) {
        if (data instanceof GroupBy || data instanceof ProbeGroupData ||
          data instanceof HostData || data instanceof ProbeData ||
          //LH - TxnData is a bit funny since commanding/standalone servers
          //     should have a record, but dist servers shouldn't. checking
          //     isn't worth it, so we don't require one
          data instanceof TxnData ||
          data instanceof IndexData ||
          data instanceof AppData ||
          data instanceof AppContentsData ||
          data instanceof AbstractMQData ||
          data instanceof CollectionData ||
          data instanceof CollectionLeakData ||
          data instanceof CodeResourceData ||

          //LH - AppMetricsData may not have a record since you can have apps
          //     that don't contain SRs, which is where the latency record
          //     comes from.
          data instanceof AppMetricsData ||
          
          // MonitorData is generic and doesn't require a latency record
          data instanceof MonitorData ||

          // We create BizTxnDatas to attach some NII, but they don't get their record until the SR happens,
          // and that might not be until after we run in to the persistence entity with no record checking.
          data instanceof BizTxnData ||

          // ClientMonitorData has metrics not latency
          data instanceof ClientMonitorData
          ) {
          return false;
        }
        return true;
      }



      public boolean exitNode(Level level) {
        return false;
      }
    }
    );
  }



  /**
   * Update the holder with the instance trees for the given node if it is a SR node.
   *
   * Only the trees that may be stored will be added. Specifically, tagged trees
   * that do not have any callees will not be candidates for storage.
   */
  private List<StoredTreeRecord> getInstanceTreeRecords(ITreeNode node,
    IGranularity granularity, int bucketIndex) {

    INodeData data = node.getData();

    List<StoredTreeRecord> records = null;

    // If it is a data type that can have trees then process them.
    if (data instanceof FragmentData || data instanceof MethodData || data instanceof ServiceData) {

      Iterator<INode> children = node.getChildIterator(TreeData.class);

      while (children.hasNext()) {
        INode child = children.next();

        StoredTreeRecord treeRecord = (StoredTreeRecord)
          ((BucketedAggregateTreeNode)child).getRecord(granularity, bucketIndex);

        if (treeRecord != null) {

          //don't store cross vm trees that don't have callees
          if (treeRecord.isMissingCallees()) {
            continue;
          }

          // LH - why is it OK to have a tree record with no block. Well,
          //      strictly speaking it shouldn't happen. However, due to the
          //      way instance tree selector handles cross-vm tracing, it is
          //      too risky (2 days before hurricane EA build) to fix. The
          //      problem is due to a couple of things. First, a x-vm tree is
          //      not selected (meaning StoredTreeRecord.setInstance() is called
          //      to make that tree the active selected tree) until it is
          //      correlated. This is done to prevent "missing" trees caused
          //      by replacing prior to any correlation being done. Secondly,
          //      the way the code is written the record for the tree is created
          //      at the very begining of the process, rather than when it is
          //      needed. The combination of these two issues means that when
          //      a callee tree is encountered, we create a TAGGED_TREE node
          //      to hold that tree, but don't set the instance on it until
          //      the instance is correlated to something else. If this
          //      correlation never happens (for all the usual reasons), we
          //      are left with the TAGGED_TREE node in the tree, without the
          //      tree for it.
          //
          //      This is OK! We don't report cross vm trees for callees, only
          //      root callers, even if it had been correlated since none of
          //      its callees (if there were any) would be correlated to that
          //      record (they go to the record for the root).
          //
          //      So, by handling this problem by simply skipping them, the only
          //      problem we are left with is the TAGGED_TREE_ data blocks in
          //      the OC for callee's. Not creating these is a risk IMO since
          //      they have always been created. Changing the existing code to
          //      not create the record makes the (already confusing) interfaces
          //      more confusing since in some cases (which would be very well
          //      hidden from the interfaces) it would be OK to not have a
          //      record (null), whereas in all others one is required. I feel
          //      that not creating the record would make the code too
          //      unmaintanable and introduces too much risk.
          //
          //      So, for now, we simply skip any tree records for which there
          //      is no tree.
          if (null == treeRecord.getBlock()) {
            continue;
          }

          if (null == records) {
            records = new ArrayList<StoredTreeRecord>();
          }
          records.add(treeRecord);
        }
      }
    }
    return records;
  }



  /**
   * Generates a queryprocessor which is an object that knows how to
   * find nodes in persistence based on query.
   * It also preprocesses the query by snapping it's times to buckets etc...
   * @param query
   * @param granularity
   * @param startTime
   * @param endTime
   * @param path
   * @param listener
   * @param combinedQuery is the query combined across OnlineCache and persistence
   */
  public IQueryProcessor createQueryProcessor(IQuery query,
    IPersistenceGranularity granularity, long startTime, long endTime,
    ArrayList<IQueryPathElement> path,
    IBucketedAggregateTreeNode.BucketedRecordMatcher listener,
    boolean combinedQuery) {

    try {
      IQueryProcessor qp = QueryProcessor.createQueryProcessor(this, query, granularity,
        startTime, endTime, path, combinedQuery);

      return qp;
    }
    catch (DurationNotFoundException e) {
      // do nothing
    }
    catch (IOException e) {
      Loggers.ARCHIVE.severe("IO error processing query: " + e, e);
    }

    return null;
  }



  /**
   * Performs an actual persistence query by using the processor
   * which is provided
   * @param processor
   * @param listener
   */
  @InstrumentationPoint(layer = "Persistence")
  public void processQuery(IQueryProcessor processor, FindNodesTraverser.IMatchListener listener) {

    synchronized (queryLock) {
      try {
        compressionBlocker.beginBlock();
        processor.process(listener);
        metrics.queryTimePrefetchBuffersPopulated.increment(processor.getUsedPrefetchBuffers());
        metrics.queryTimePrefetchBuffersReused.increment(processor.getReusedPrefetchBuffers());
        metrics.persistedQueries.increment();
      }
      catch (IOException e) {
        Loggers.ARCHIVE.severe("IO error processing query: " + e, e);
      }
      catch (InterruptedException e) {
        Loggers.ARCHIVE.warning("Can't acquire reader lock: " + e, e);
      }
      finally {
        compressionBlocker.endBlock();
      }
    }
  }



  public MajorDuration getSourceGranularity(IGranularityData granularityData) {
    try {
      MajorDuration major = storageManager.getMajorForGranularity(granularityData);
      int idx = major.getPrevMajor();

      if (-1 != idx) {
        return storageManager.getMajorDurations()[idx];
      }
      else {
        return null;
      }
    }
    catch (DurationNotFoundException e) {
      return null;
    }
  }



  public IPersistenceGranularity getGranularity(final IGranularityData granularityData, final long startTime) {
    try {
      final MajorDuration major = storageManager.getMajorForGranularity(granularityData);

      return new PersistenceGranularity(this, granularityData, major, startTime);

    }
    catch (DurationNotFoundException e) {
      return null;
    }
  }



  /**
   * get the name of the metric group provided by this class
   */
  public String getMetricProviderName() {
    return "Persistence";
  }



  public StorageManager getStorageManager() {
    return storageManager;
  }



  public ISymbolManager getSymbolManager() {
    return symbolManager;
  }



  public ServerTimeManager getTimeManager() {
    return timeManager;
  }



  /**
   * Does a clean shutdown of persistence
   */
  public void shutdown() {
    symbolManager.shutdown();

  }



  /**
   * Use to shutdown the UT instance of persistence.
   */
  void shutdownUT() {
    shutdown();

    storageManager.delete();
    (new File(UT_PATHED_FILE_NAME)).deleteOnExit();
  }



  /**
   * Given an iterator over a source summary, and a writable iterator for target,
   * this method will rewrite the source into the target. This essentially copies
   * the node holder's from the source iterator into the target, but also takes care
   * of properly adjusting child positions.
   *
   * @param source the summary we are rewriting
   * @param target target destination
   * @throws java.io.IOException
   */
  public void rewriteSummary(DurationSummaryIterator source, WritableDurationSummaryIterator target) throws IOException {
    // We are not reaggregating.
    PersistenceAggregationContext context = new PersistenceAggregationContext();

    context.doingReaggregation = false;

    // Use empty iterator for original duration, since we are simpy rewriting
    // the source (durationFileIter param) into the target (durationWriter param).
    // No need for recordMap nor trendFile params so use null there.
    List<NodePositions> parentPositions = aggregateIteratorToFile(EmptyIterator.EMPTY, source, target, null, -1, null, context, false, 0);

    // Now update the child positions for all the rewritten nodes
    updateChildPositions(target, parentPositions);

    // Copy the last trend offset from source to target as that did not chnage
    target.updateLastTrendOffsetPosition(source.getLastTrendOffsetPosition());
  }



  /**
   * Given an iterator over summary, whether it's a memory nodeholders representation or an actual
   * DurationSummaryIterator over a file, and given the target reader/writer iterators, aggregate the source
   * summary into the target.
   *
   * @param originalDur - the source duration (online node holders, or reaggregation source)
   * @param durationFileIter - (re)aggregation destination - the iterator with preexisting data
   * @param durationWriter - output destination - .write file
   * @param offset
   * @param trendFile
   * @param isTotal - true if the summary being aggregated is the TOTAL
   * @param purgingTimeStamp - if isTotal, any entity that has a last modified
   *                           time before the purgingTimesStamp will be deleted
   *                           from the system. (age-based purging implementation)
   */
  @InstrumentationPoint(layer = "Persistence")
  private List<NodePositions> aggregateIteratorToFile(
    Iterator<NodeHolder> originalDur, DurationSummaryIterator durationFileIter,
    WritableDurationSummaryIterator durationWriter,
    TreeSet<TrendMapper> recordMap, int offset, TrendFile trendFile,
    PersistenceAggregationContext context, boolean isTotal,
    long purgingTimeStamp) throws IOException {

  	boolean doTreeAggregation = ! context.major.getName().equals(treeCutOffMajor); // allows us to skip trees for higher majors if enabled
  	
    //LH - we need to get the deleted node holders so we can delete their children
    durationFileIter.setReturnDeletedNodeHolders(true);
    if (originalDur instanceof DurationSummaryIterator) {
      ((DurationSummaryIterator)originalDur).setReturnDeletedNodeHolders(true);
    }

    Loggers.ARCHIVE.debug("aggregating to : " + durationWriter.file + " isTotal: " + isTotal);
    
    // Mark the current trend size. If we fail during writing the summary iterator (durationWriter)
    // we can reset the trend file to its original size
    long currentTrendFileSize = -1;

    if (trendFile != null) {
      currentTrendFileSize = trendFile.getTrendFileSize();
    }
    
    try {
	    
      HashSet<Long> deletedPathKeys = new HashSet<Long>();
      List<NodePositions> parentPositions = new ArrayList<NodePositions>();
	
      int newNodes = 0;
      int oldNodes = 0;
      int aggregatedNodes = 0;
	
      NodePositions parentPos = new NodePositions();
	
      NodeHolder newHolder = null;
      NodeHolder existingNodeHolder = null;
	
      boolean progressNewHolder = true;
      boolean progressPersist = true;
	
      while (durationFileIter.hasNext() || originalDur.hasNext()) {
	
        if (context.doingReaggregation) {
          waitForAsynchBucketComplete();
        }
	
        if (recordMap != null) {
          if (durationWriter.getAppendBufferSize() > recordChunk || recordMap.size() > recordChunk) {
            // For majors
            parentPositions = durationWriter.matchParentsToBuffer(parentPositions);
            writeTrends(recordMap, trendFile, offset, context, durationWriter);
            updateTrendPositionsForMajor(durationWriter, recordMap);
            recordMap.clear();
          }
        }
        else {
          //  For minors (no trends)
          if (durationWriter.getAppendBufferSize() > recordChunk) {
            parentPositions = durationWriter.matchParentsToBuffer(parentPositions);
            durationWriter.flushAppends();
          }
        }
        while (progressNewHolder && newHolder == null && originalDur.hasNext()) {
          newHolder = originalDur.next();
          if (symbolManager.containsPathKey(newHolder.getPathedKey())) { // entity was not deleted
            boolean deleted = deleteChildrenOfDeletedKeys(deletedPathKeys, newHolder);
	
            if (!deleted) {
              if (!context.doingReaggregation) {
                newHolder = new NodeHolder(newHolder);
              }
              if (recordMap != null) {
                newHolder.resetTrendPositions();
              }
              progressNewHolder = false;
            }
            else {
              newHolder = null;
            }
          }
          else {
            deletedPathKeys.add(newHolder.getPathedKey());
            newHolder = null;
          }
        }
	
        while (progressPersist && existingNodeHolder == null && durationFileIter.hasNext()) {
          existingNodeHolder = durationFileIter.next();
          if (symbolManager.containsPathKey(existingNodeHolder.getPathedKey())) { // entity was not deleted
            boolean deleted = deleteChildrenOfDeletedKeys(deletedPathKeys, existingNodeHolder);
	
            if (deleted) {
              existingNodeHolder = null;
            }
            else {
              progressPersist = false;
            }
          }
          else {
            deletedPathKeys.add(existingNodeHolder.getPathedKey());
            existingNodeHolder = null;
          }
        }
	
        int diff;
	
        if (newHolder != null && existingNodeHolder != null) {
          diff = nodeHolderComparator.compare(existingNodeHolder, newHolder);
        }
        else if (newHolder != null) {
          diff = 1;
        }
        else if (existingNodeHolder != null) {
          diff = -1;
        }
        else {
          //the entity has been deleted, so we need to just skip it
          continue;
        }
	
        long nodeHolderTimeStamp = 0;
        long pathKey = 0;
	
        if (diff < 0) {
          //entity doesn't exist in the new summary, simply copy it from the existing one
          nodeHolderTimeStamp = existingNodeHolder.getLastModified();
          if (nodeHolderTimeStamp == 0 && isTotal) {
            existingNodeHolder.setLastModified(timeManager.getCurrentTargetTime());
          }
          durationWriter.append(existingNodeHolder, true);
          pathKey = existingNodeHolder.getPathedKey();
          parentPos = updateParentPositions(parentPos, existingNodeHolder, parentPositions);
          progressNewHolder = false;
          progressPersist = true;
          existingNodeHolder = null;
          oldNodes++;
        }
        else if (diff == 0) {
          //exists in both existing and new
	
          NodeHolder recordNodeHolder = null;
	
          if (recordMap != null && newHolder.fields != 0) {
            recordNodeHolder = new NodeHolder(newHolder);
            recordNodeHolder.aggregateTrendPositions(existingNodeHolder);
            buildRecordMap(recordMap, recordNodeHolder);
          }
	
          newHolder.aggregate(existingNodeHolder);
	
          if (!isTotal) {
          	if (doTreeAggregation) 
          		newHolder.aggregateTrees(existingNodeHolder, context);
          }
	
          nodeHolderTimeStamp = newHolder.getLastModified();
	
          if (nodeHolderTimeStamp == 0 && isTotal) {
            newHolder.setLastModified(timeManager.getCurrentTargetTime());
          }
	
          durationWriter.append(newHolder, true);
          pathKey = newHolder.getPathedKey();
	
          if (recordNodeHolder != null) {
            recordNodeHolder.setWrittenAtPosition(newHolder.getWrittenAtPosition());
          }
	
          parentPos = updateParentPositions(parentPos, newHolder, parentPositions);
	
          progressNewHolder = true;
          progressPersist = true;
          existingNodeHolder = null;
          newHolder = null;
	
          aggregatedNodes++;
        }
        else if (diff > 0) {
	
          //exists in new, but not existing
	
          if (!isTotal) {
          	if (doTreeAggregation) 
          		newHolder.aggregateTrees(null, context);
          	
          }
	
          nodeHolderTimeStamp = newHolder.getLastModified();
	
          if (nodeHolderTimeStamp == 0 && isTotal) {
            newHolder.setLastModified(timeManager.getCurrentTargetTime());
          }
	
          durationWriter.append(newHolder, true);
          pathKey = newHolder.getPathedKey();
	
          parentPos = updateParentPositions(parentPos, newHolder, parentPositions);
          if (recordMap != null && newHolder.fields != 0) {
            buildRecordMap(recordMap, newHolder);
          }
          progressNewHolder = true;
          progressPersist = false;
          newHolder = null;
          newNodes++;
        }
	
        // TODO: This age-based purging has caused some problems, mostly when combined
        // TODO: with defects in the system that caused the reaggregation to not occur
        // TODO: for long perdiods of time, say at least 3 months. Under such circumstances,
        // TODO: the 'last modified' timestamps on any node holder here is going to
        // TODO: be less than purgingTimeStamp, causing deletion of the symbol.
        if (isTotal && nodeHolderTimeStamp < purgingTimeStamp && nodeHolderTimeStamp > 0) {
          symbolManager.deletePathKey(pathKey, true);
          clearQueryResultCache();
        }
	
      }
	
      //the last parent has to be added to the map
      if (parentPos != null && parentPos.pathKey != 0) {
        parentPositions.add(parentPos);
      }
	
      if (recordMap != null) {
        if (recordMap.size() > 0) {
          parentPositions = durationWriter.matchParentsToBuffer(parentPositions);
          writeTrends(recordMap, trendFile, offset, context, durationWriter);
          updateTrendPositionsForMajor(durationWriter, recordMap);
          recordMap = new TreeSet<TrendMapper>();
        }
      }
      else {
        if (durationWriter.getAppendBufferSize() > 0) {
          parentPositions = durationWriter.matchParentsToBuffer(parentPositions);
          durationWriter.flushAppends();
        }
      }
	
      if (Loggers.ARCHIVE.isEnabledFor(Level.DEBUG)) {
        Loggers.ARCHIVE.debug("New - " + newNodes + ", Old " + oldNodes + ", Aggreg " + aggregatedNodes);
      }
	
      if (!context.doingReaggregation && trendFile != null) {
        metrics.trendRecordsStored.increment(trendFile.getTrendRecordsWrittenMetric());
      }
	    
      return parentPositions;
    
    } // end try
    catch (IOException e) {
      // We got an exception (probably out of disk space). Truncate the trend file back to its original size
      // since we may have trends written to it without updating references to the trends in the summary file
      if (currentTrendFileSize > 0) {
        trendFile.truncate(currentTrendFileSize); // file will be closed
      }
	    
      throw e; // re-throw
    }
    
  }



  /**
   * Deletes the path key of the node holder if an ancestor has been deleted.
   * @param deletedPathKeys
   * @param newHolder
   * @return
   */
  private boolean deleteChildrenOfDeletedKeys(HashSet<Long> deletedPathKeys, NodeHolder newHolder) {
    boolean deleted = false;

    if (deletedPathKeys.size() > 0) {
      long path[] = symbolManager.getParentPathFromPathKey(newHolder.getPathedKey());

      for (long parentsKey : path) {
        if (deletedPathKeys.contains(parentsKey)) {
          symbolManager.deletePathKey(newHolder.getPathedKey(), true);
          deletedPathKeys.add(newHolder.getPathedKey());
          deleted = true;
        }
      }
    }

    clearQueryResultCache();

    return deleted;
  }



  /**
   * Iterates through parents array and the shadow file and tells each parent where its children are
   *
   * @param durationWriter the summary file that we're creating right now
   * @param parentPositions the list of parents that have updated positions (that need to be written to the file)
   */
  private void updateChildPositions(WritableDurationSummaryIterator durationWriter, List<NodePositions> parentPositions) throws IOException {

    //  make sure nothig is outstanding (we're about to read this file)
    durationWriter.flushAppends();

    // core operation is that we're looping through two ordered sets (ordering is implied by the design)

    NodePositions parentPos = null;
    NodeHolder nodeHolderPersist = null;
    boolean progressParentPositions = true;
    boolean progressDurationWriter = true;

    durationWriter.reset();
    Iterator<NodePositions> parentsIter = parentPositions.iterator();

    while (durationWriter.hasNext() || parentsIter.hasNext()) {

      if (progressParentPositions && parentPos == null && parentsIter.hasNext()) {
        parentPos = parentsIter.next();
      }

      while (progressDurationWriter && nodeHolderPersist == null && durationWriter.hasNext()) {
        nodeHolderPersist = durationWriter.next();
      }

      if (parentPos != null) {
        if (parentPos.pathKey == 0) {
          durationWriter.updateTopLevelLastNodePosition(parentPos.lastChildPosition);
        }
      }

      if (parentPos != null && nodeHolderPersist != null) {

        int diff = nodeHolderComparator.compare(nodeHolderPersist, parentPos);

        if (diff < 0) {
          progressParentPositions = false;
          progressDurationWriter = true;
          nodeHolderPersist = null;
        }
        else if (diff == 0) {
          //  found a parent that is in the array -- write out the child begin/end locations
          durationWriter.updateChildrenPosition(parentPos, nodeHolderPersist.getWrittenAtPosition());
          progressParentPositions = true;
          progressDurationWriter = true;
          nodeHolderPersist = null;
          parentPos = null;
        }
        else if (diff > 0) {
          progressParentPositions = true;
          progressDurationWriter = false;
          parentPos = null;
        }
      }
      else if (parentPos == null && !parentsIter.hasNext()) {
        break;
      }
      else if (!progressParentPositions && !durationWriter.hasNext()) {
        break;
      }
    }
  }



  /**
   * Updates the child offsets for the parents in summaries.
   *
   * @param parent
   * @param nodeHolderPersist
   * @param parentPositions
   */
  private NodePositions updateParentPositions(NodePositions parent,
    NodeHolder nodeHolderPersist, List<NodePositions> parentPositions) {
    long parentPathKey = symbolManager.getParentComponentOfPathValue(symbolManager.getPathedValue(nodeHolderPersist.getPathedKey()));
    long writtenPos = nodeHolderPersist.getWrittenAtPosition();

    if (parent.pathKey != parentPathKey) {
      parentPositions.add(parent);
      parent = new NodePositions();
      parent.pathKey = parentPathKey;
    }

    if (parent.firstChildPosition > writtenPos || parent.firstChildPosition == -1) {
      parent.firstChildPosition = writtenPos;
    }
    if (parent.lastChildPosition < writtenPos) {
      parent.lastChildPosition = writtenPos;
    }

    return parent;
  }



  /**
   * Creates a treeset of trendmapper objects that are nodeholder wrappers holding the holders
   * ordered in a trend position order, each nodeholder is referenced as many times as the number of it's trended fields
   *
   * @param recordMap
   * @param holder
   */
  private void buildRecordMap(TreeSet<TrendMapper> recordMap, NodeHolder holder) {
    for (int i = 0; i < TrendedFields.FIELD_COUNT; i++) {
      int field = 1 << i;

      if (holder.isFieldTrended(field) && !holder.mustHaveEmptyTrend(field)) {
        TrendMapper mapper = new TrendMapper(field, holder.getTrendPosition(field), holder);
   
        recordMap.add(mapper);
      }
    }
  }



  /**
   * @deprecated take it as a Pico object instead
   */
  public QueryLock getQueryLock() {
    return queryLock;
  }



  /**
   * Consolidate values to maxTrendPoints.
   *
   * @param maxTrendPoints
   * @param field see {@link com.mercury.diagnostics.common.data.graph.TrendedFields}
   * @param values
   * @return new consolidated array to of maxTrendPoints length
   */
  static double[] consolidateTrend(int maxTrendPoints, int field, double[] values) {
    // The implementation was moved to common location:
    return TrendFunctions.consolidateTrend(maxTrendPoints, field, values);
  }



  public QueryProcessor.Cached getQueryCacheResult(CachedQueryKey queryKey) {
    QueryProcessor.Cached ret = null;

    synchronized (queryResultCache) {
      ret = (QueryProcessor.Cached)queryResultCache.get(queryKey);

      //check that the last modified times on the keys match as well
      if (null != ret) {
        if (queryKey.getBucketLastModified() != ret.getKey().getBucketLastModified()) {
          ret = null;
          removeFromQueryResultCache(queryKey);
        }
        else {
          Loggers.QUERY.debug("got cached result for : " + queryKey + ": " + ret);
        }
      }

      if (null != ret) {
        metrics.cacheHits.increment();
      }
      else {
        metrics.cacheMisses.increment();
      }
    }
    return ret;
  }



  /**
   * the listener is ready to have its data cached, so get it and add it to the
   * cache.
   */
  void cacheQueryResults(CachedQueryKey key, QueryProcessor.Cached toCache) {
    Loggers.QUERY.debug("caching result for : " + key + ": " + toCache);
    synchronized (queryResultCache) {
      queryResultCache.put(key, toCache);
    }
  }



  public void clearQueryResultCache() {
    //LH - we acquire the query lock here in order to prevent a query that
    //     is currently being processed from ending up in the cache, basically,
    //     wait until a running query is done, then flush the cacche.
    synchronized (queryLock) {
      synchronized (queryResultCache) {
        queryResultCache.clear();
      }
    }
  }



  public void removeFromQueryResultCache(CachedQueryKey key) {
    synchronized (queryResultCache) {
      queryResultCache.remove(key);
    }
  }



  public boolean isCompressionAllowed() {
    return allowCompression;
  }



  /**
   * get the multiplier for the topN amount. This value will be >= 1.0 and is
   * used to increase the topN for persistence so extra entities will be cached
   * to try and avoid invalidating the cache when a new item appears in OC topN
   */
  public double getTopNMultiplier() {
    return topNMultiplier;
  }

  /**
   * A granularity for persistence. In addition to the standard granularity
   * functionality, this class also knows about and exposes the major/minor hiearchy.
   */
  public static class PersistenceGranularity extends IGranularity.Default implements IPersistenceGranularity {
    private final IGranularityData granularityData;
    private final MajorDuration major;
    private long startTime;
    private long endTime;
    private Persistence persistence;

    public PersistenceGranularity(Persistence persistence, IGranularityData granularityData, MajorDuration major, long startTime) {
      this.persistence = persistence;
      this.granularityData = granularityData;
      this.major = major;
      this.startTime = startTime;
      this.endTime = startTime + getBucketDuration() - 1;
    }



    public int getIndex() {
      return granularityData.getIndex();
    }



    public IGranularityData getTemplate() {
      return granularityData;
    }



    public IGranularity getPreviousGranularity() {
      return persistence.getGranularity(persistence.storageManager.getPreviousGranularityData(major, granularityData), startTime);
    }



    public long getStartTime() {
      return startTime;
    }



    public long getEndTime() {
      return endTime;
    }



    public void setEndTime(long endTime) {
      this.endTime = endTime;
    }



    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }



    public long getBucketDuration() {
      return granularityData.getDuration(startTime);
    }



    public long getTrendResolution() {
      return major.getTrendResolution(persistence.storageManager.getMajorDurations(), startTime);
    }



    public ITrendManager getTrendManager(int bucketIndex) {
      // Persistence doens't have a trend manage - at least yet anyways :^)
      return null;
    }



    public List<Integer> getBucketIndices(boolean includeRolling, boolean includeCompleted) {
      // Since persistence has not trend managers, return an empty list.
      return Collections.EMPTY_LIST;
    }



    public boolean isRollingBucket(int bucketIndex) {
      return false;
    }



    public IPersistenceGranularity getSourcePersistedGranularity() {
      int majorIdx = major.getPrevMajor();

      if (-1 != majorIdx) {
        MajorDuration prevMajor = persistence.storageManager.getMajorDurations()[majorIdx];
        int minorIdx = major.getPrevMinor();

        GranularityData prevMinor = prevMajor.getMinorDurations()[minorIdx];

        return new PersistenceGranularity(persistence, prevMinor, prevMajor, startTime);
      }
      else {
        return null;
      }
    }



    public String toString() {
      return "duration=" + getBucketDuration() + ", trendResolution=" + getTrendResolution();
    }
  }




  private class PersistenceMetrics {

    private static final String CACHE_HITS_METRIC_DESC = "metric.description.persistence.cache.hits";
    private static final String CACHE_MISSES_METRIC_DESC = "metric.description.persistence.cache.misses";
    private static final String CACHE_HIT_RATIO_METRIC_DESC = "metric.description.persistence.cache.hit.ratio";
    private static final String PERSISTENCE_TREE_NODES_STORED = "metric.description.persistence.tree.nodes.stored";
    private static final String PERSISTENCE_TREND_RECORDS_STORED = "metric.description.persistence.trend.records.stored";

    final LongCounterMetric cacheHits;
    final LongCounterMetric cacheMisses;
    final RatioMetric cacheRatio;

    final LongCounterMetric queryTimePrefetchBuffersPopulated = new LongCounterMetric("", "", "");
    final LongCounterMetric queryTimePrefetchBuffersReused = new LongCounterMetric("", "", "");
    final LongCounterMetric persistedQueries = new LongCounterMetric("Queries", "Persistence", "");

    final RatioMetric queryTimePrefetchUsedPerQuery;
    final RatioMetric queryTimePrefetchReusedPerQuery;

    final LongCounterMetric treeNodesStored;
    final LongCounterMetric trendRecordsStored;

    PersistenceMetrics(IResourceManager resourceManager) {
      cacheHits = new LongCounterMetric("cache", "hits", resourceManager.getString(CACHE_HITS_METRIC_DESC));
      cacheMisses = new LongCounterMetric("cache", "misses", resourceManager.getString(CACHE_MISSES_METRIC_DESC));

      CompoundLongCounterMetric total = new CompoundLongCounterMetric("", "", "");

      total.addMetric(cacheHits);
      total.addMetric(cacheMisses);
      cacheRatio = new RatioMetric("cache", "hit ratio", resourceManager.getString(CACHE_HIT_RATIO_METRIC_DESC), cacheHits, total);

      queryTimePrefetchUsedPerQuery = new RatioMetric("Populated", "Prefetch Buffers", "", queryTimePrefetchBuffersPopulated, persistedQueries);
      queryTimePrefetchReusedPerQuery = new RatioMetric("Reused", "Prefetch Buffers", "", queryTimePrefetchBuffersReused, persistedQueries);

      treeNodesStored = new LongCounterMetric("Stored", "Tree Nodes", resourceManager.getString(PERSISTENCE_TREE_NODES_STORED));
      trendRecordsStored = new LongCounterMetric("Stored", "Trend Records", resourceManager.getString(PERSISTENCE_TREND_RECORDS_STORED));
    }



    /** register the metrics with the monitor */
    public void register(IMonitor monitor) {
      monitor.addMetric(Persistence.this, cacheHits);
      monitor.addMetric(Persistence.this, cacheMisses);
      monitor.addMetric(Persistence.this, cacheRatio);
      monitor.addMetric(Persistence.this, queryTimePrefetchUsedPerQuery);
      monitor.addMetric(Persistence.this, queryTimePrefetchReusedPerQuery);
      monitor.addMetric(Persistence.this, persistedQueries);
      monitor.addMetric(Persistence.this, treeNodesStored);
      monitor.addMetric(Persistence.this, trendRecordsStored);
    }

  }

}




/**
 * A simple object that encapsulates the position at which a nodeholder was written in the summary
 * and where it's first and last children are written.
 */
class NodePositions {
  public long firstChildPosition = -1;
  public long lastChildPosition = -1;
  public long pathKey = 0;
  private long[] path = null;

  public synchronized long[] getPath(ISymbolManager manager) {
    if (path == null) {
      path = manager.getParentPathFromPathKey(pathKey);
    }
    return path;
  }

}




/**
 * A wrapper class that holds the nodeholder that will be written to a trend for a field at a position.
 * The order of these objects (as defined by the Comparable implementation) is by increasing trend position.
 * Unspecified (-1) trendPositions will all be at the beginning of the sorting.
 *
 * LH - shouldn't the unspecified positions be at the end, so that we write all
 *      the trends in the existing portion of the file, then append the new
 *      trends. Currently (with -1 at the beginning), we will append all the new
 *      trends, then go back and write all the existing trends.
 *
 * LH - the javadoc is wrong. The sort order is strictly reverse numerical order
 *      of trendPosition. All of the -1s are at the end, but the specified
 *      positions will be written backwards. So, when we update the trend file
 *      we start at the end, go to the begining, and then extend from the end.
 */
class TrendMapper implements Comparable<TrendMapper> {

  public final int field;
  public final NodeHolder holder;

  public long trendPosition;
  public boolean written = false;

  public TrendMapper(int field, long trendPosition, NodeHolder holder) {
    this.field = field;
    this.trendPosition = trendPosition;
    this.holder = holder;
  }



  public int compareTo(TrendMapper other) {

  	if (other == this) { // Java 1.7 needs this
  		return 0; 
  	}
  	
    if (trendPosition == -1) {
      return 1; //if this is unspecified, it is always greater
    }
    else {
      if (other.trendPosition == -1 || trendPosition < other.trendPosition) {
        return -1;
      }
      else if (trendPosition == other.trendPosition) {
        throw new IllegalStateException("if this happens, we will drop a trend");
      }
      else {
        return 1;
      }
    }
  }
}

