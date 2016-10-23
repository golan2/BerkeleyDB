/*
 * (c) Copyright 2009 Hewlett-Packard Development Company, L.P. 
 * --------------------------------------------------------------------------
 * This file contains proprietary trade secrets of Hewlett Packard Company.
 * No part of this file may be reproduced or transmitted in any form or by
 * any means, electronic or mechanical, including photocopying and
 * recording, for any purpose without the expressed written permission of
 * Hewlett Packard Company.
 */


package com.mercury.diagnostics.server.persistence.symboltable.impl;


import com.mercury.diagnostics.common.data.graph.EntityKey;
import com.mercury.diagnostics.common.data.graph.INode;
import com.mercury.diagnostics.common.data.graph.ISymbolManager;
import com.mercury.diagnostics.common.data.graph.impl_oo.NodeDataPathKey;
import com.mercury.diagnostics.common.data.graph.impl_oo.WritableReader;
import com.mercury.diagnostics.common.data.graph.node.GroupBy;
import com.mercury.diagnostics.common.io.*;
import com.mercury.diagnostics.common.loader.AbstractModuleObject;
import com.mercury.diagnostics.common.logging.Level;
import com.mercury.diagnostics.common.logging.Loggers;
import com.mercury.diagnostics.common.modules.monitoring.IMetricProvider;
import com.mercury.diagnostics.common.modules.monitoring.IMonitor;
import com.mercury.diagnostics.common.modules.timemanager.TimeManager;
import com.mercury.diagnostics.common.modules.webserver.handlers.ISpecialFileHandler;
import com.mercury.diagnostics.common.util.InfrequentLogger;
import com.mercury.diagnostics.common.util.PropertiesUtil;
import com.mercury.diagnostics.server.persistence.IStorageManager;
import com.sleepycat.je.DatabaseException;
import org.mortbay.util.Resource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;


/**
 * An implemintation of the ISymbolManager interface
 * Includes the references to the managed files, and provides over the file
 * system implementation of local and pathed keys.
 *
 * Symbol Table Aging: (not yet implemented, documented for future use)
 *   symbol table aging will essentially be a copying garbage collector. Each
 *   generation period will create a new symbol table, and as entries encountered
 *   in the old generation, they will be migrated to the new. Only active entries
 *   will be moved. Once a reaggregation has completed, the old generation can
 *   be deleted.
 *
 *   For symbols that reference other symbols (for example, the FragmentArcData
 *   source field refers to the local key of the source fragment), it is the
 *   burden of the refering data to update the last modified time of the
 *   referred symbol whenever it is updated.
 *
 *   When the server exposes a local key it will do so in such a way that the
 *   generation the symbol was created in is encoded in the local key. The
 *   purpose of this is to ensure that any references to the key do not incorrectly
 *   resolve to a different entity that was recreated after an old symbol
 *   was purged and then reused.
 *
 */
public class SymbolManager extends AbstractModuleObject
  implements IMetricProvider, ISymbolManager, ISpecialFileHandler {

  /**
   * The cache of local keys
   */
  private IntNodeDataCrossRef localNodeDataCache;

  /**
   * The node data cache. This cache contains all of the data objects that are
   * currently in the OC data tree, as well as any there were recently and
   * haven't been removed yet, in persistence cache....basically, the repository
   * for node datas.
   *
   * It is used to ensure that the OC has a way of getting info updates from
   * the symbol manager.
   *
   * 1) we can find the "official" data to refer to when creating nodes, this
   *    "official" data will be the one that is updated by the symbol manager
   *    when data info update notifications are received.
   * 2) when nothing refers to the data, the entry in the map will be GC'ed.
   *
   * Why can't we simply use the localNodeDataCache? Well, that cache is an
   * MRU, and does not take the OC into account, or use reference tracking to
   * maintain references to any in-use datas. The problem with this is that the
   * OC will have data's that need to be updated with info updates. In order to
   * do this, we need to have a single instance of the data that we can update,
   * this is done by using this map. Since it is a WeakHashMap, only the datas
   * that are still in use somewhere in the system will be cached (long term).
   *
   * LH - the value needs to be wrapped in a WeakReference to prevent it from
   *      preventing itself being released as the key.
   *
   */
  private final WeakHashMap<NodeIdentifier, WeakReference<INodeData>> nodeDataReferenceCache;

  /**
   * The cache of all groupby datas (keyed be path key). This allows us to avoid scanning
   * of the pathed symbol table every time cam discovery asks for all groupbys.
   */
  private final Map<Long, GroupBy> cachedGroupBys = new HashMap<Long, GroupBy>();

  /**
   * The class that manages persistency of local keys
   */
  private ILocalSymbolTable localSymbolTable;

  /**
   * The class that manages persistency of pathed keys
   */
  private PathAwareSymbolFile pathAwareSymbolFile;

  /**
   * Current enumerator for the colliding local keys
   */
  private int currentMin = Integer.MAX_VALUE;

  /**
   * Current enumerator for the colliding pathed keys
   */
  private int currentIntMin = Integer.MAX_VALUE;

  /**
   * Gathers statistics of symbol table
   */
  private final SymbolManagerStats stats;

  /**
   * preallocated int array for getting the path
   */
  private transient long[] tmpPath = new long[1];

  private final Object tmpPathProtector = new Object();

  private List<ISymbolDeleteListener> deleteListeners = new ArrayList<ISymbolDeleteListener>();

  public static final long symbolManagerStartupTime = System.currentTimeMillis();

  private static enum ManagerState {
    INIT, // Object created, still initialized.
    READY, // Ready!  everything is initialized.
    SHUTDOWN // Trying to shutdown, no more symbol lookups.
  }

  /**
   * What state is the symbol manager in?
   */
  private volatile ManagerState managerState = ManagerState.INIT;

  public SymbolManager(PropertiesUtil properties, IStorageManager storageManager,
    TimeManager timeManager, ByteBufferConfig bbc,
    String pathAwareSymbolFileName, IMonitor monitor)
    throws SymbolManagerInitializationException, DatabaseException {

  
    if (null == monitor) {
      stats = new SymbolManagerStats();
    }
    else {
      stats = new SymbolManagerStats(monitor);
    }

    nodeDataReferenceCache = new WeakHashMap<NodeIdentifier, WeakReference<INodeData>>();
    
    try {
    
      localSymbolTable = new LocalSymbolTable(properties, storageManager, this, timeManager, bbc);
  
      Loggers.ARCHIVE.debug("Starting module symbol table");
  
      long reservedMemorySize = properties.getProperty("symboltable.reserved.memory", 10);
      int  mruListSize = properties.getProperty("symboltable.mru.size", 5000);
     
      localNodeDataCache = new IntNodeDataCrossRef(reservedMemorySize, mruListSize);
  
      if (null == pathAwareSymbolFileName) {
        pathAwareSymbolFileName =
          properties.getResolvedProperty("symboltable.pathed.filename",
          "${archive.dirname}/${mediator.id}/${symboltable.dirname}/PathedSymbolTable.db");
      }
      pathAwareSymbolFile = new PathAwareSymbolFile(pathAwareSymbolFileName, stats);
  
      //LH - we *really* shouldn't set ready until we are done with initializing
      //     the table, which includes the call to initializeSymbolTable(). However
      //     that calls back into this class during the upgrade, and we need to
      //     be in READY state for that to succeed. Setting it to that state early
      //     is OK since we are in a constructor, and anything else won't have
      //     a reference to this object yet (since it really isn't constructed).
      managerState = ManagerState.READY;
  
      localSymbolTable.initializeSymbolTable(true);
  
      // Cache the existing groupbys
      cacheGroupBys();
  
      Loggers.ARCHIVE.debug("Module symbol table ready");
  
      //  Prepopulate some things in the symbol table in a consistent order so that
      //  the path keys are ALWAYS the same.  This helps ensure our unit-tests are
      //  consistent.
      //    INodeData groupBy = new GroupBy(GroupBy.DEFAULT_CLIENT, GroupBy.EMPTY_RUN, GroupBy.MODE_AM);
      //    this.getPathKey(Arrays.asList(groupBy, new ProbeGroupData("Default")));
      
    	}
  	
  	catch (SymbolManagerInitializationException t)
  	{
  		Loggers.ARCHIVE.severe("Unable to create archive", t);
  		throw t;
  	}
    catch (DatabaseException t)
  	{
  		Loggers.ARCHIVE.severe("Unable to create archive", t);
  		throw t;
  	}
  }



  /**
   * This is the constructor that is actually invoked by Pico/Module Loader.
   */
  public SymbolManager(PropertiesUtil properties, IStorageManager storageManager,
    TimeManager timeManager, ByteBufferConfig bbc, IMonitor monitor)
    throws SymbolManagerInitializationException, DatabaseException {
    this(properties, storageManager, timeManager, bbc, null, monitor);
  }



  /**
   * Constructor for unit test
   *
   * @param localSymbolTable the table to use, or null if one should be created
   */
  public SymbolManager(String pathAwareSymbolFileName,
    ILocalSymbolTable localSymbolTable, long reservedMemorySize)
    throws SymbolManagerInitializationException, DatabaseException {

    stats = new SymbolManagerStats();

    nodeDataReferenceCache = new WeakHashMap<NodeIdentifier, WeakReference<INodeData>>();
    localNodeDataCache = new IntNodeDataCrossRef(reservedMemorySize);
    pathAwareSymbolFile = new PathAwareSymbolFile(new File(pathAwareSymbolFileName).getAbsolutePath(), stats);

    if (null != localSymbolTable) {
      this.localSymbolTable = localSymbolTable;
    }
    else {
      this.localSymbolTable = LocalSymbolTable.createUTInstance(this);
    }
    managerState = ManagerState.READY;

    // Cache the existing groupbys
    cacheGroupBys();
  }



  /**
   * Creates an instance of symbol manager for dumper utility
   */
  public static SymbolManager getDumperInstance(PropertiesUtil propUtil, IStorageManager storage,
    TimeManager timeManager) throws SymbolManagerInitializationException, DatabaseException {
    Loggers.ARCHIVE.debug("Starting module symbol table");

    String pathAwareSymbolFileName;
    
    if (propUtil.getProperty("archive.dirname") != null) {
    	pathAwareSymbolFileName =
    		propUtil.getResolvedProperty("symboltable.pathed.filename",
    		"${archive.dirname}/${mediator.id}/${symboltable.dirname}/PathedSymbolTable.db");
    }
    else if ( System.getProperty("install.dir")!=null ) {
    	String absoluteRoot = new File(System.getProperty("install.dir")).getAbsolutePath();

    	pathAwareSymbolFileName = absoluteRoot + File.separator +
    		propUtil.getResolvedProperty("symboltable.pathed.filename",
    		"storage/symboltable/PathedSymbolTable.db");
    }

    else {
      pathAwareSymbolFileName = "C:\\Users\\golaniz\\Desktop\\Misc\\QCIM1I108840 - LLoyds\\APPLGVIRTUAL2BD_Days\\Days";
    }
    
    return new SymbolManager(propUtil, storage, timeManager,
      null, pathAwareSymbolFileName, null);
  }



  public String getMetricProviderName() {
    return "SymbolTable";
  }



  /**
   * Calculate sync with caches/persistency and provide local key
   * This will only work if there is only one thread setting the Symbols
   * // todo: if multithreaded, properly lock/synchronize
   *
   * @param create should an entry be created. if false, and the key doesn't
   *               exist, NoSuchElementException is thrown
   *
   * @throws java.util.NoSuchElementException
   */
  public int getLocalKey(INodeData data, boolean create) {
    int lk = unsafeGetLocalKey(data, create);

    if (lk == UNSPECIFIED_LOCAL_KEY) {
      throw new NoSuchElementException();
    }
    return lk;
  }



  private int unsafeGetLocalKey(INodeData data, boolean create) {
    int tmpId = data.getNodeIdentifier().getSymbolToken();

    if (UNSPECIFIED_LOCAL_KEY != tmpId) {
      return tmpId;
    }

    synchronized (this) {
      if (ManagerState.READY != managerState) {
        Loggers.ARCHIVE.warning("Trying to assign key to symbol table " +
          "in invalid state: " + managerState);
        throw new IllegalStateException("Can not assign keys in invalid symbol " +
          "table state: " + managerState);
      }

      //get the key from the reference cache (if available)
      INodeData fromRefCache = getNodeDataReference(data);

      if (null != fromRefCache) {   // The data in cache and has a token to return
        int localKey = fromRefCache.getNodeIdentifier().getSymbolToken();

        data.getNodeIdentifier().setSymbolToken(localKey);
        return localKey;
      }

      tmpId = localNodeDataCache.get(data); // See if it's in MRU cache

      if (tmpId != NodeDataFactory.UNSPECIFIED_LOCAL_KEY) { //The cache returned a valid data with token
        stats.readHit.increment();
        data.getNodeIdentifier().setSymbolToken(tmpId);
        return tmpId;
      }

      tmpId = data.hashCode(); // Assign optimistic key

      INodeData tmpVal = localNodeDataCache.get(tmpId); // Do we have a data with this key already?

      if (tmpVal != null) {
        // We do, and it's not the one we look for (cause none of the data to
        // key cache lookups matched), therefore it's a collision, we have a
        // different value for this id;
        int collId = localSymbolTable.getFromCollisionList(data); // Do we have a known collision for it?

        if (collId != UNSPECIFIED_LOCAL_KEY) { //we do, return the known collision
          stats.readFromCollision.increment();
          data.getNodeIdentifier().setSymbolToken(collId);
          return collId;
        }

        // it's a collision, but it is not in collision list - we don't have a known collision for this
        if (!create) {
          return UNSPECIFIED_LOCAL_KEY;
        }
        writeCollidingValue(data, tmpId); // write it as a new collision

        //  writeCollidingValue decrements currentMin, so it now points at a new ID

        data.getNodeIdentifier().setSymbolToken(currentMin); // assign the new min to it.
        return currentMin;
      }
      else {
        // we know that it's not in our cache, try to look up from DB
        INodeData dbVal = localSymbolTable.getValue(tmpId);

        INodeData tmpCopy = data;

        if (tmpCopy instanceof GroupBy) {
          tmpCopy = ((GroupBy)data).createSymbolManagerCopy();
        }

        if (dbVal == null) { // The database didn't have a data for this id, therefore,
          // the id is new, safe for usage;
          if (!create) {
            return UNSPECIFIED_LOCAL_KEY;
          }
          localSymbolTable.setValue(tmpId, data);
          localNodeDataCache.put(tmpId, tmpCopy);
          nodeDataReferenceCache.put(tmpCopy.getNodeIdentifier(), new WeakReference<INodeData>(tmpCopy));
          stats.writeNew.increment();
          
          if (stats.writeNew.longValue() % 1000 == 0) { // Log a message when we're writting lots of symbols to disk (e.g. caused by incorrect URL trimming)
            InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE, "local symbols written", null, 60 * 60 * 1000).info("Number of local symbols (entities) written to disk since server start: " + stats.writeNew.longValue());
          }
          
          data.getNodeIdentifier().setSymbolToken(tmpId);
          return tmpId;
        }

        // If the db has the same value for this key, simply return the key
        if (dbVal.nodeIdentifiersEquals(data)) {
          stats.readFromFile.increment();
          localNodeDataCache.put(tmpId, dbVal);
          nodeDataReferenceCache.put(dbVal.getNodeIdentifier(), new WeakReference<INodeData>(dbVal));
          data.getNodeIdentifier().setSymbolToken(tmpId);
          return tmpId;
        }

        // The key's in the DB, but the values don't match, it's a collision -

        // Check if it's allready in collision list
        int collId = localSymbolTable.getFromCollisionList(data);

        if (collId != UNSPECIFIED_LOCAL_KEY) { // We have a collision id for this data
          stats.readFromCollision.increment();

          if (collId < currentMin) { // Probably never occurs, a sanity against strange skippages in the DB
            currentMin = collId;
          }

          if (Loggers.ARCHIVE.isEnabledFor(Level.DEBUG)) {
            Loggers.ARCHIVE.debug("Collision detected " + collId + ", " + data + " vs " + tmpId + ", " + localSymbolTable.getValue(tmpId));
          }

          data.getNodeIdentifier().setSymbolToken(collId);
          return collId;
        }

        if (!create) {
          return UNSPECIFIED_LOCAL_KEY;
        }

        writeCollidingValue(data, tmpId);
        //  writeCollidingValue decrements currentMin, so it now points at a new ID

        data.getNodeIdentifier().setSymbolToken(currentMin);
        return currentMin;
      }
    }
  }



  public void infoUpdated(INodeData data) {
    infoUpdated(data, true);
  }



  /**
   * @throws java.util.NoSuchElementException if the data isn't already in the symbol table
   */
  public synchronized void infoUpdated(INodeData data, boolean forceSync) {
    int localKey = data.getNodeIdentifier().getSymbolToken();

    if (NodeDataFactory.UNSPECIFIED_LOCAL_KEY == localKey) {
      localKey = getLocalKey(data, false);

      //  What if localKey still == UNSPECIFIED_LOCAL_KEY?  The below setValue
      //  is going to do really strange things, isn't it?
    }

    localSymbolTable.setValue(localKey, data, forceSync);

    INodeData fromCache = getNodeDataReference(data);

    if (null != fromCache && data != fromCache) {
      //only copy the info if the updated data isn't the one from the cache
      fromCache.replaceInfo(data);
    }

    localNodeDataCache.put(localKey, null == fromCache ? data : fromCache);

  }



  public synchronized <T extends INodeData> T getNodeDataFromReferenceCache(T nodeData) {
    T ret = getNodeDataReference(nodeData);

    if (null == ret) {
      int localKey = getLocalKey(nodeData, true);

      ret = (T)getValue(localKey);
    }
    return ret;
  }



  /**
   * Returns zero if a key for this data has not already been assigned
   */
  public int hasLocalKey(INodeData data) {
    return unsafeGetLocalKey(data, false);
  }



  /**
   * Calculate, sync and assign a pathed key
   *
   * @param parentKey The parent path key
   * @param localId The local key
   * @return the newly assigned path key
   */
  public long assignKey(long parentKey, int localId) {

    if (ManagerState.READY != managerState) {
      Loggers.ARCHIVE.warning("Trying to assign key to symbol table " +
        "in invalid state: " + managerState);
      throw new IllegalStateException("Can not assign keys in invalid symbol " +
        "table state: " + managerState);
    }

    //String value = attrVals.getValue();
    int tmpId = ApHash.combine(ApHash.getPKFromPKGen(parentKey), localId); // ideal PK
    long value = ApHash.concat(ApHash.getPKFromPKGen(parentKey), localId); // 'pathed value', aka PV

    synchronized (pathAwareSymbolFile) {
      
      long existingId = pathAwareSymbolFile.get(parentKey, localId); // todo: can throw a runtime exception due to deleted/reassigned parent
  
      if (existingId == 0) {
        
  
        if (ApHash.getGeneration(pathAwareSymbolFile.getPathKeyWithGeneration(tmpId)) > 0) {
          // collision here
          if (currentIntMin == Integer.MAX_VALUE) {
            currentIntMin = pathAwareSymbolFile.takeNextMinId();
          }
          else {
            --currentIntMin;
          }

          tmpId = currentIntMin;
        }

        try {
          int generationId = pathAwareSymbolFile.writeSymbol(tmpId, value);

          existingId = ApHash.concat(tmpId, generationId);

          /* this is too noisy to have on by default
           if (Loggers.ARCHIVE.isEnabledFor(Level.DEBUG)) {
           Loggers.ARCHIVE.debug("Assigning path key: generationId[" + generationId + "] pathKey[" + existingId + "] for parentKey[" + parentKey + "] localId[" + localId + "]");
           }
           */
        }
        catch (IOException e) {
          throw new RuntimeException("Symbol creation failure during assignment of path keys " + e, e);
        }
        
  
        // We just assigned a new path key. If the node is a groupby node, we need to cache it
        if (NodeDataFactory.UNSPECIFIED_PATH_KEY == parentKey) {
  
          // Resolve the local id into node data.
          INodeData data = getValue(localId);
  
          if (data instanceof GroupBy) {
            cachedGroupBys.put(existingId, ((GroupBy)data).convertToAmGroupBy());
          }
          else {
            Loggers.ARCHIVE.severe("Schema violation: non-groupby found at root of tree" + (null == data ? "." : ": " + data.describe()));
          }
        }
      }
      return existingId;
    }

    

  }



  /**
   * Checks if the provided path key is 'incomplete', that is, whether or
   * not it includes the generation id. This method allows you to determie
   * if the path key is a pre-Hurricane path key.
   */
  public boolean isIncompletePathKey(long pathKey) {
    // Generation is a low int in the long. If the path key is pre-Hurricane,
    // then the path key itself will be the low int.
    return ApHash.getGeneration(pathKey) == pathKey;
  }



  /**
   * A method to fill a generaion for a path key without generation,
   * but only if the path key is of old gen. If it is of new gen, a runtime exception is thrown
   * @param incompletePathKey
   * @return
   */
  public long fillInGenerationForOldGen(int incompletePathKey) throws DeletedEntityException {
    return pathAwareSymbolFile.getPathKeyWithGenerationAndAssertOldGen(incompletePathKey);
  }



  /**
   * For a path key w/o generation, returns the fully qualified path key with generation id.
   * @param incompletePathKey
   * @return path key with generation id
   */
  public long getPathKeyWithGeneration(int incompletePathKey) {
    return pathAwareSymbolFile.getPathKeyWithGeneration(incompletePathKey);
  }



  /**
   * A utility method to return a fully qualified parent key with generation
   * for a given pathed value
   * @param pathedValue
   * @return
   */
  public long getParentComponentOfPathValue(long pathedValue) {
    return pathAwareSymbolFile.getParentKey(pathedValue);
  }



  public long getParentPathKeyFromPathKey(long childPathKey) {
    long pathedVal = getPathedValue(childPathKey);
    long parentpk = getParentComponentOfPathValue(pathedVal);

    return parentpk;
  }



  /**
   * Very slow mechanism for getting all children of a node.  Need to add result caching
   * or some other data structure if this is queried often.
   * @return An array of child local keys.
   */
  public int[] getChildLocalKeys(long parentKey) {
    return pathAwareSymbolFile.getChildLocalKeys(parentKey);
  }

  

  /**
   * Slow mechanism for getting all children for a set of nodes -- do not call in a loop!
   * (For example, this would be a truly terrible way of iterating the entire PST)
   * 
   * @return A map of children by parent key
   */
  public Map<Long, List<Integer>> getChildLocalKeys(long[] parentKeys) {
    return pathAwareSymbolFile.getChildLocalKeys(parentKeys);
  }



  /**
   * Deletes the entry from memory, and blanks the entry on disk
   * @param pathKey - the key to be deleted
   * @return true if success, false if failure
   */
  public boolean deletePathKey(long pathKey, boolean doEntityLookup) {
  	
    long pathedValue;
    StringBuffer buf = new StringBuffer();
    
    try {
      pathedValue = getPathedValue(pathKey);
      
      if (doEntityLookup) {
        
        INodeData data = getNodeDataFromPathKey(pathKey);
        NodeIdentifier identifier = data.getNodeIdentifier();
        
        buf.append(data.getClass().getName()).append(": ");
        
        if (identifier == null)
          buf.append(data.toString());
        else
          buf.append(identifier.toString());
      }
    } catch (NoSuchElementException e1) {
      // no need to go any further if it's already gone
      return true;
    }

    buf.append(" (").append(pathKey).append(',').append(pathedValue).append(')');

    try {
      // debug start
    	/*
           try {
       List<INodeData> path = getNodeDataPathFromPathKey(pathKey);
       String message = "Deleteing path key " + Long.toHexString(pathKey) + " with path ";
       for (INodeData data:path) {
       message += data.getDisplayName() + " > ";
       }
       System.err.println(message);
       try {
       throw new RuntimeException();
       } catch (RuntimeException e) {
       StackTraceElement[] elems = e.getStackTrace();
       String stackMsg = "";
       for (StackTraceElement elem : elems) {
       stackMsg += elem.getClassName() + "." + elem.getMethodName() + " -> ";
       }
       System.err.println("      From: " + stackMsg);
       }
       } catch (Exception e) {
       e.printStackTrace();
       }          */          
      // debug end
      Loggers.ENTITY_PURGING.info("Deleting entity: " + buf.toString());
      if (pathAwareSymbolFile.deleteKey(pathKey)) {

        // There is a chance that the path key is for a groupBy.
        // We don't delete often, so it is cheaper to simply try this
        // that to figure our if the entity actually is a groupby.
        cachedGroupBys.remove(pathKey);

        for (ISymbolDeleteListener listener : deleteListeners) {
          listener.pathKeyDeleted(pathKey);
        }

        return true;
        
      }
      else {
        return false;
      }
    }
    catch (IOException e) {
      Loggers.ENTITY_PURGING.severe("Deletion of entity " + buf.toString() + " failed due to " + e, e);
      return false;
    }
  }



  public void addPathsToEntityKey(EntityKey entityKey) {
    ArrayList<INodeData> path = entityKey.getNodeDataPath();
    ArrayList<Long> pathKeys = new ArrayList<Long>();

    long parentPath = NodeDataFactory.UNSPECIFIED_PATH_KEY;

    for (INodeData data: path) {
      parentPath = assignKey(parentPath, getLocalKey(data, true));
      pathKeys.add(parentPath);
    }

    entityKey.setPathKeys(pathKeys);
  }



  /**
   * Convenience method to assign local and path keys to a node.
   * Will assign a local key to node if not already assigned.
   * Will assign a path key to node using parentNode pathkey if not already assigned.
   *
   * @param node
   * @param parentNode
   */
  public void assignKeys(INode node, INode parentNode) {

    //  NOT SYNCHRONIZED -- all methods called from here are synchronized

    node.getData().getNodeIdentifier().setSymbolToken(getLocalKey(node.getData(), true));

    long parentPathKey = 0;

    if (parentNode != null) {
      parentPathKey = parentNode.getPathKey();
    }

    long pathKey = assignKey(parentPathKey, node.getData().getNodeIdentifier().getSymbolToken());

    node.setPathKey(pathKey);
  }



  /**
   * Persists a colliding local key
   *
   * @param value
   * @param localKey
   */
  private synchronized void writeCollidingValue(INodeData value, int localKey) {
    // initialize collision index
    if (ManagerState.READY != managerState) {
      Loggers.ARCHIVE.warning("Trying to write colliding value to symbol table " +
        "in invalid state: " + managerState);
      throw new IllegalStateException("Can not write colliding value in invalid " +
        "symbol table state: " + managerState);
    }

    if (currentMin == Integer.MAX_VALUE) {
      currentMin = localSymbolTable.takeNextMinId();
    }
    else {
      --currentMin;
    }

    //  ATOMIC -- could be skipping entries if OOM in writeSymbol (currentMin already decremented)
    localSymbolTable.setValue(currentMin, value);
    localNodeDataCache.put(currentMin, value);
    nodeDataReferenceCache.put(value.getNodeIdentifier(), new WeakReference<INodeData>(value));
    stats.writeCollision.increment();

    if (Loggers.ARCHIVE.isEnabledFor(Level.DEBUG)) {
      Loggers.ARCHIVE.debug("Collision detected " + currentMin + ", " + value + " vs " + localKey + ", " + localSymbolTable.getValue(localKey));
    }
  }



  /**
   * returns the node data with the specified token. The returned value is
   * guaranteed to exist in the reference cache and to receive info updates.
   *
   * @param token the token for the data to retrieve (returned from createSymbolToken)
   */
  public synchronized INodeData getValue(int token) {
    if (ManagerState.READY != managerState) {
      Loggers.ARCHIVE.warning("Trying to read value from symbol table " +
        "in invalid state: " + managerState);
      throw new IllegalStateException("Can not access symbol table in " +
        "invalid state: " + managerState);
    }

    return _getValue(token);
  }



  /**
   * Bypasses the state check, callable while the LocalSymbolTable is initializing.
   *
   * @param token
   * @return
   */
  private synchronized INodeData _getValue(int token) {
    INodeData value = null;

    boolean cacheMiss = false;

    value = localNodeDataCache.get(token);
    if (value == null) {
      cacheMiss = true; // we have a cache miss for localDataNodeCache, indicate that we have to update the cache further down
      value = localSymbolTable.getValue(token);
    }

    // This is bad unless... the symbol was purged.
    if (value == null) {
      throw new NoSuchElementException("Cannot find node data for token " + token);
    }

    //check to see if the data is in the cache
    INodeData fromRefCache = getNodeDataReference(value);

    if (null != fromRefCache) {
      value = fromRefCache; 
    }
    else {
      nodeDataReferenceCache.put(value.getNodeIdentifier(), new WeakReference<INodeData>(value));
    }
    
    // update the cache if we had a miss
    if (cacheMiss) {
      localNodeDataCache.put(token, value);
    }

    /**
     * RunId is not part of nodeidentifier on groupbys, and therefore have
     * to be invalidated when read from symboltable, as it is not
     * mutable in symbol manager, but has to be compared to in equals of groupby
     */
    if (value instanceof GroupBy) {
      ((GroupBy)value).invalidateRunId();
    }

    return value;
  }



  private <T extends INodeData> T getNodeDataReference(T value) {
    WeakReference<INodeData> ref = nodeDataReferenceCache.get(value.getNodeIdentifier());
    INodeData fromRefCache = null != ref ? ref.get() : null;

    return (T)fromRefCache;
  }



  public void addDataByKey(Class c, Object key, INodeData nodeData) {
    throw new UnsupportedOperationException();
  }



  public INodeData getDataByKey(Class c, Object key) {
    throw new UnsupportedOperationException();
  }



  public INodeData intern(INodeData data) {
    return getNodeDataFromReferenceCache(data);
  }



  /**
   * Returns the path key for an entity with a given data path.
   * @param dataPath
   * @return
   * @throws IllegalArgumentException if dataPath is null
   */
  public long getPathKey(List<INodeData> dataPath) {
    return getPathKey(dataPath, true);
  }



  /**
   * Returns the path key for an entity with a given data path, or zero if it doesn't already exist.
   * @param dataPath
   * @return The pathkey for the entity, or 0 (NodeDataFactory.UNSPECIFIED_PATH_KEY) if it doesn't exist right now
   * @throws IllegalArgumentException if dataPath is null
   */
  
  private long getPathKeyOrZero(List<INodeData> dataPath, boolean create) {

    
    if (dataPath == null) {
      throw new IllegalArgumentException();
    }

    long pathKey = 0;

    for (INodeData data : dataPath) {
      int localKey = data.getNodeIdentifier().getSymbolToken();

      if (localKey == 0) {
        localKey = unsafeGetLocalKey(data, create);
        if (localKey == UNSPECIFIED_LOCAL_KEY) {
          return UNSPECIFIED_PATH_KEY;
        }
      }
      // first lookup from cache.  only call assignKey which needs added synchronization, if there is entry does not exist.   
      long existingPathKey = pathAwareSymbolFile.get(pathKey, localKey);
      
      if (existingPathKey != UNSPECIFIED_PATH_KEY) {
        pathKey = existingPathKey;        
      } else if (existingPathKey == UNSPECIFIED_PATH_KEY && create) {
        pathKey = assignKey(pathKey, localKey);      
      } else {
        pathKey = UNSPECIFIED_PATH_KEY;
      }
    }

    return pathKey;
  }

  

  /**
   * Returns the path key for an entity with a given data path.
   * @param dataPath
   * @return
   * @throws IllegalArgumentException if dataPath is null
   * @throws java.util.NoSuchElementException if create is false and an entry doesn't exist for a data or path relationship
   */
  public long getPathKey(List<INodeData> dataPath, boolean create) {

    long pathKey = getPathKeyOrZero(dataPath, create);

    if (pathKey == UNSPECIFIED_PATH_KEY) {
      if (!dataPath.isEmpty()) {
        //  Actually, zero is a valid PK for an empty path.
        throw new NoSuchElementException();
      }
    }

    return pathKey;
  }



  /**
   * Returns the path key for an entity with a given data path, or zero if it doesn't already exist.
   * @param dataPath
   * @return The pathkey for the entity, or 0 (NodeDataFactory.UNSPECIFIED_PATH_KEY) if it doesn't exist right now
   * @throws IllegalArgumentException if dataPath is null
   */
  public long getExistingPathKeyOrZero(List<INodeData> dataPath) {
    return getPathKeyOrZero(dataPath, false);
  }



  public SymbolManagerStats getStats() {
    return stats;
  }



  /**
   * Is this path key in the table
   * @param parentKey
   * @param localKey
   * @return
   */
  public boolean containsPathKey(long parentKey, int localKey) {
    return pathAwareSymbolFile.contains(parentKey, localKey);
  }



  public long getPathKey(long parentKey, int localKey) {
    return pathAwareSymbolFile.get(parentKey, localKey);
  }



  /**
   * Is this path key in the table
   * @param pathedKey
   * @return
   */

  public boolean containsPathKey(long pathedKey) {
    return pathAwareSymbolFile.containsKey(pathedKey);
  }



  /**
   * This a convinience method for getting a concatenated string containing the path of local values
   * based on the pathed key.
   * <b>This is hideously expensive and should not be used outside debug level messages and unitests</b>
   *
   * @param key
   */
  public synchronized String getStringPathFromPathKey(long key) {
    StringBuffer buffer = new StringBuffer();
    long withParent = pathAwareSymbolFile.getValue(key);

    while (withParent != 0) {

      long parentKey = getParentComponentOfPathValue(withParent);
      int localId = ApHash.getLocalKey(withParent);
      String localName = getValue(localId).getName();

      withParent = pathAwareSymbolFile.getValue(parentKey);
      if (withParent != 0) {
        localName = " -> " + localName;
      }

      buffer.insert(0, localName);
    }
    return buffer.toString();
  }



  /**
   * A utility method to get a full path of parent pathed keys from one pathed key
   *
   * @param key
   */
  public long[] getParentPathFromPathKey(long key) {

    //int [] path = new int[0];
    //path[0] = key;
    long[] path;
    int length = 0;
    long startKey = key;
    
    //todo: fix and add protection for parent generation being lower than children
    synchronized (tmpPathProtector) {
      if (key != 0) {
        do {
          long withParent = pathAwareSymbolFile.getValue(key);

          int keyWithoutGen = ApHash.getParentKey(withParent);

          key = pathAwareSymbolFile.getPathKeyWithGeneration(keyWithoutGen);
          length++;

          // Path way too long, we must have detected a cycle...
          if (length > 100) {
            
            StringBuilder sb = new StringBuilder();
            
            for (int i = 0; i < 100; i++) {
              if (i > 0) {
                sb.append(",");
              }
              sb.append(tmpPath[i]);
            }
            
            InfrequentLogger.getInfrequentLogger(Loggers.ARCHIVE, "PST Cycle for " + Long.toString(startKey), 
                null, 10 * 60 * 1000).severe("Cycle in generating path for " + Long.toString(startKey) + " from PK, path = " + sb.toString());
            
            throw new IllegalStateException();
          }

          if (tmpPath.length < length) {
            long[] tmp = new long[tmpPath.length + 1];

            System.arraycopy(tmpPath, 0, tmp, 1, length - 1);
            tmpPath = tmp;
          }
          tmpPath[tmpPath.length - length] = key;
        }        
        while (key != 0);
      }
      path = new long[length];
      System.arraycopy(tmpPath, tmpPath.length - length, path, 0, length);
    }

    return path;
  }



  /**
   * A utility method to get a full path of local keys from one pathed key
   * This is not concurrent with writes, and therefore doesn't need to be synchronized
   * @param key
   */
  public int[] getLocalKeysFromPathKey(long key) {

    int[] path = new int[0];

    if (key != 0) {
      do {
        int[] tmp = new int[path.length + 1];

        System.arraycopy(path, 0, tmp, 1, path.length);
        path = tmp;
        path[0] = ApHash.getLocalKey(getPathedValue(key));

        long withParent = pathAwareSymbolFile.getValue(key);

        key = pathAwareSymbolFile.getPathKeyWithGeneration(ApHash.getParentKey(withParent));
      }
      while (key != 0);
    }

    return path;
  }

  


  /**
   * Returns the metadata for a given path key
   * @param key
   */
  public long getKeyMetaData(long key) {
    return pathAwareSymbolFile.getKeyMetaData(key);
  }



  /**
   * Sets the metadata of a given path key
   * @param key
   * @param meta
   */
  public void setKeyMetaData(long key, long meta) {
    pathAwareSymbolFile.setKeyMetaData(key, meta);
  }



  /**
   * Flushes out the path keys, done before onlinebucket complete for correctenss
   * @throws java.io.IOException
   */
  public void forcePathKeys() throws IOException {
    pathAwareSymbolFile.force();
  }



  /**
   * Get <code>INodeData</code> from a path key.
   *
   * @param pathKey
   */
  public INodeData getNodeDataFromPathKey(long pathKey) {

    //  NOT SYNCHRONIZED -- all methods called from here are synchronized

    return getValue(ApHash.getLocalKey(getPathedValue(pathKey)));
  }



  /**
   * Constructs the entity key for a node based on its path key.
   * @param pathKey
   */
  public EntityKey getEntityKeyFromPathKey(long pathKey) {

    ArrayList<INodeData> nodeDatas = new ArrayList<INodeData>();
    ArrayList<Long> pathKeys = new ArrayList<Long>();

    getPathKeys(pathKey, pathKeys, nodeDatas);

    NodeDataPathKey ret = new NodeDataPathKey(nodeDatas);

    ret.setPathKeys(pathKeys);

    return ret;
  }



  /**
   * Returns a list of all groupbys found in the path symbol table.
   */
  public synchronized Collection<GroupBy> getAllGroupBys() {

    // We have the always up-to-date cache of groupbys (just for this purpose), so use it
    return Collections.unmodifiableCollection(cachedGroupBys.values());
  }



  /**
   * Caches all groupbys found in the path symbol table.
   */
  private void cacheGroupBys() {

    // For every child node of the 'root' (those are the groupbys)
    for (int groupByLK : getChildLocalKeys(NodeDataFactory.UNSPECIFIED_PATH_KEY)) {

      // Resolve it into node data
      INodeData data = getValue(groupByLK);

      // If should be a groupby, however, just to be sure...
      if (data instanceof GroupBy) {
        cachedGroupBys.put(getPathKey(NodeDataFactory.UNSPECIFIED_PATH_KEY, groupByLK), ((GroupBy)data).convertToAmGroupBy());
      }
    }
  }



  /**
   * get the list of path keys leading up to and including pathKey. If nodeDatas
   * is non-null, the datas for each level will be added to that list as well.
   */
  private synchronized void getPathKeys(long pathKey, ArrayList<Long> pathKeys, ArrayList<INodeData> nodeDatas) {

    if (pathKey != NodeDataFactory.UNSPECIFIED_PATH_KEY) {
      do {
        pathKeys.add(0, pathKey);
        if (null != nodeDatas) {
          nodeDatas.add(0, getValue(ApHash.getLocalKey(getPathedValue(pathKey))));
        }
        long withParent = pathAwareSymbolFile.getValue(pathKey);

        pathKey = pathAwareSymbolFile.getPathKeyWithGeneration(ApHash.getParentKey(withParent));
      }
      while (pathKey != NodeDataFactory.UNSPECIFIED_PATH_KEY);
    }

  }



  /**
   * Constructs the node data path for a node based on its path key.
   * @param pathKey
   */
  public List<INodeData> getNodeDataPathFromPathKey(long pathKey) {

    //  NOT SYNCHRONIZED -- all methods called from here are synchronized

    final List<INodeData> nodes = new ArrayList<INodeData>();

    for (int localKey : getLocalKeysFromPathKey(pathKey)) {
      nodes.add(getValue(localKey));
    }

    return nodes;
  }



  // No need to synchronize this method, if all we do PSTFile lookup - which is already internally synchronized.   
  // Otherwise we generate lock contention accessing the PST with the other synchronized methods in this class that
  // do actual LST/DB lookups from disk...
  
  public long getPathedValue(long key) {
    
    return pathAwareSymbolFile.getValue(key);
        
  }
  


  /**
   * Does a clean shutdown of the SymbolManager which closes the files
   */
  public synchronized void shutdown() {
    managerState = ManagerState.SHUTDOWN;
    localSymbolTable.close();
    pathAwareSymbolFile.close();
  }



  protected static long length(AbstractSymbolFile symbolFile) {
    synchronized (symbolFile.symbolFileChannel) {
      try {
        return symbolFile.symbolFileChannel.size();
      }
      catch (IOException e) {
        Loggers.ARCHIVE.warning("SymbolTable resource query failed" + e, e);
        throw new RuntimeException("SymbolTable resource query failed" + e, e);
      }
    }
  }



  /**
   * Implementation for <code>ISpecialFileHandler</code>, returns the 
   * paths for the symbol table files.
   *
   * @return a <code>String[]</code>
   */
  public File[] getSpecialFiles() {
    return new File[] { pathAwareSymbolFile.getFile(),
    };
  }



  public Resource getResource(File file) {
    if (file.getName().equals(pathAwareSymbolFile.getFile().getName())) {
      return new SymbolTableHttpResource(pathAwareSymbolFile);
    }

    return null;
  }



  /**
   * Get iterator over local table
   * @return
   */
  public ClosingIterator<INodeData> getSymbolTableIterator() {
    return localSymbolTable.getSymbolTableIterator();
  }



  /**
   * Get iterator over pathed table
   * @return
   */
  public IntLongIntHashIterator getPathedSymbolTableIterator() {
    return pathAwareSymbolFile.getSymbolTableIterator();
  }



  /**
   * Get an iterator over only those pathed symbols that belong the the
   * provided customer and have local key that matches the provided local key.
   * If the specified local key is <code>NodeDataFactory.UNSPECIFIED_LOCAL_KEY</code>
   * then the mathcing is not done and the iterator will walk over all
   * nodes under the groupby.
   *
   * This method can be used, for example, to get all status nodes for a
   * given customer.
   *
   * TODO: This needs a more efficient implementation (some caching will be required)
   */
  public Iterator<Map.Entry> getPathedSymbolTableIterator(GroupBy customer, final int localKeyToMatch) {

    // Local key of the groupby
    final int groupByLK = customer.getNodeIdentifier().getSymbolToken();

    // Get the 'normal' iterator over all symbols
    final Iterator<Map.Entry> iterator = pathAwareSymbolFile.getSymbolTableIterator();

    // Return an iterator that 'wraps' the above and only returns elements
    // which are descendents of the provided groupby.
    return new Iterator<Map.Entry>() {

      private Map.Entry nextEntry = null;

      public boolean hasNext() {

        // I guess someone called 'hasNext' twice in a row.
        if (this.nextEntry != null) {
          return true;
        }

        // Walk until we find an entry with matching local key (if that is what the caller wanted)
        // and a matching groupby root node.
        while (iterator.hasNext()) {
          Map.Entry nextEntry = iterator.next();

          if (localKeyToMatch == NodeDataFactory.UNSPECIFIED_LOCAL_KEY || localKeyToMatch == ApHash.getLocalKey((Long)nextEntry.getValue())) {

            long pathKey = getPathKeyWithGeneration((Integer)nextEntry.getKey());

            if (groupByLK == pathAwareSymbolFile.getGroupByLocalKey(pathKey)) {
              this.nextEntry = nextEntry;
              return true;
            }
          }
        }

        // Got here therefore we've got nothing
        return false;
      }



      public Map.Entry next() {

        // If we have next, then it is 'cached' in this.nextEntry.
        // Nullify it before returning
        if (hasNext()) {
          Map.Entry entryToReturn = this.nextEntry;

          this.nextEntry = null;
          return entryToReturn;
        }

        throw new NoSuchElementException();
      }



      public void remove() {
        throw new UnsupportedOperationException("Remove is not supported.");
      }
    };
  }



  public IWritable read(IWritable.DataIn in, short id) throws IOException {
    if (WritableReader.ID_WritableToken != id) {
      throw new IllegalStateException();
    }
    int localKey = in.readInt();

    // A call-back from the local symbol table to read can by-pass the
    // state check.
    INodeData ret = _getValue(localKey);

    return ret;
  }



  public void write(IWritable.DataOut out, IWritable obj, String fieldName) throws IOException {
    if (!(obj instanceof INodeData)) {
      throw new IllegalArgumentException("Symbol Manager can only write INodeData");
    }

    out.writeClassId(WritableReader.ID_WritableToken);
    out.writeInt(getLocalKey((INodeData)obj, true), "token");
  }



  public void writeStructure(IWritable.DataOut out, IWritable obj, String fieldName) throws IOException {
    throw new UnsupportedOperationException("Symbol Manager can't write structure, only INodeData");
  }



  public void registerDeleteListener(ISymbolDeleteListener listener) {
    deleteListeners.add(listener);
  }

  

  /**
   * Add at raw pathed symbol table record to the path aware symbol table.
   * Used only for merging pathed symbol tables files.
   * 
   * @param pathKey - pathkey (with generation) of the record
   * @param key - first int in the record key The parent-PK and LK hashed together
   * @param pathedValue - first long in the record (value, parent pk in upper 32, local key in lower 32 bits)
   * @param metaData - second long of the record (metadata)
   * @throws java.io.IOException
   */
  public void updateSymbol(long pathKey, int key, long pathedValue, long metaData) throws IOException {
    pathAwareSymbolFile.updateSymbol(pathKey, key, pathedValue, metaData);    
  }
  


  public int getGenerationId(int key) {
    return pathAwareSymbolFile.getGenerationId(key); 
  }
  


  /**
   * A utility method to return a fully qualified parent key with generation
   * for a given pathed value
   * @param pathedValue
   * @return
   */
  public long getParentKey(long pathedValue) {
    return pathAwareSymbolFile.getParentKey(pathedValue);
  }
  
  

  /**
   * Return the timestamp (in ms) when the pathed symbol table was created (used for instant-on licensing)
   * @return timestamp (in ms)
   */
  public long getPathedSymbolTableCreationTimestamp() {
    return pathAwareSymbolFile.getCreationTimestamp();
  }
  
  /**
   * Jetty resource which knows how to handle symbol table files for HTTP
   * retrieval.
   *
   * @author <a href="mailto:telliott@mercury.com"></a>
   */
  static class SymbolTableHttpResource extends Resource {

    private final AbstractSymbolFile symbolFile;

    public SymbolTableHttpResource(AbstractSymbolFile file) {
      this.symbolFile = file;
    }



    public void release() {
      // do nothing
    }



    public boolean exists() {
      return true;
    }



    public boolean isDirectory() {
      return false;
    }



    public long lastModified() {
      Calendar calendar = Calendar.getInstance();

      // We are deliberately marking Midnight as the time this file last
      // changed.  What this will do is ensure that any backup done via HTTP
      // for the symbol files occurs at least once a day.  The rest of the time
      // incremental downloads are ok.
      calendar.set(Calendar.HOUR_OF_DAY, 0);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);

      return calendar.getTimeInMillis();
    }



    public long length() {
      return SymbolManager.length(symbolFile);
    }



    public URL getURL() {
      try {
        return symbolFile.getFile().toURL();
      }
      catch (MalformedURLException murle) {
        return null;
      }
    }



    public File getFile() throws IOException {
      return symbolFile.getFile();
    }



    public String getName() {
      return symbolFile.getFile().getName();
    }



    public InputStream getInputStream() throws IOException {
      return new SymbolTableInputStream(symbolFile);
    }



    public OutputStream getOutputStream() throws IOException, SecurityException {
      throw new SecurityException("Not allowed to change symbol table");
    }



    public boolean delete() throws SecurityException {
      throw new SecurityException("Not allowed to delete symbol table");
    }



    public boolean renameTo(Resource dest) throws SecurityException {
      throw new SecurityException("Not allowed to rename symbol table");
    }



    public String[] list() {
      return new String[] { getName() };
    }



    public Resource addPath(String path)
      throws IOException, MalformedURLException {
      throw new SecurityException("No resource internal to a symbol table");
    }

  }




  /**
   * Input stream used to read the symbol table via HTTP (for backup purposes)
   *
   * @author <a href="mailto:telliott@mercury.com"></a>
   */
  static class SymbolTableInputStream extends InputStream {
    private final AbstractSymbolFile symbolFile;

    /**
     * Buffer used for reading, so we don't have to create new ones for
     * every read() operation.
     */
    private ByteBuffer readBuffer = ByteBuffer.wrap(new byte[1]);

    /**
     * Current position.
     */
    private long position = 0;

    public SymbolTableInputStream(AbstractSymbolFile symbolFile) {
      this.symbolFile = symbolFile;
    }



    public int available() {
      long length = SymbolManager.length(symbolFile);

      if (length > Integer.MAX_VALUE) {
        return Integer.MAX_VALUE;
      }

      return (int)length;
    }



    /**
     * Reads a byte from the symbol table file, and returns it.
     *
     * @return
     * @throws java.io.IOException
     */
  
    public int read() throws IOException {

      // Not synchronizing here, since whoever is reading us will have
      // synchronized to grab the length() from the Resource (above)
      // As long as the file never shrinks, we should be good.
      symbolFile.symbolFileChannel.read(readBuffer, position++);

      return (int)(readBuffer.get(0) & 0xFF);
    }



    public int read(byte[] b, int off, int len) throws IOException {

      if (len == 0) {
        return 0;
      }
      else if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) > b.length) || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      }

      ByteBuffer buf = ByteBuffer.wrap(b, off, len);

      // Not synchronizing here, since whoever is reading us will have
      // synchronized to grab the length() from the Resource (above)
      // As long as the file never shrinks, we should be good.
      int read = symbolFile.symbolFileChannel.read(buf, position);

      position += read;

      return read;
    }
  }
  
}
