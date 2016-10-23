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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;

import com.mercury.diagnostics.common.data.graph.ISymbolManager;
import com.mercury.diagnostics.common.data.graph.impl_oo.WritableReader;
import com.mercury.diagnostics.common.data.graph.impl_oo.WritableTokenizer;
import com.mercury.diagnostics.common.data.graph.node.NIIKey;
import com.mercury.diagnostics.common.io.ByteBufferConfig;
import com.mercury.diagnostics.common.io.FileUtils;
import com.mercury.diagnostics.common.io.INodeData;
import com.mercury.diagnostics.common.io.IWritable;
import com.mercury.diagnostics.common.io.NodeDataFactory;
import com.mercury.diagnostics.common.io.NodeIdentifier;
import com.mercury.diagnostics.common.logging.Level;
import com.mercury.diagnostics.common.logging.Loggers;
import com.mercury.diagnostics.common.modules.timemanager.TimeManager;
import com.mercury.diagnostics.common.util.PropertiesUtil;
import com.mercury.diagnostics.server.persistence.IStorageManager;
import com.mercury.diagnostics.server.time.ServerTimeManager;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

/**
 * Manages local symbol tokens.
 */
public class LocalSymbolTable implements ILocalSymbolTable {

	private static final String COLLISIONS_DB = "collisions";

	private static final String DATABASE_NAME = "localsymboltable";

	/** the time manager to use for tracking last modified times */
	private final TimeManager timeManager;

	/** Configuration properties. */
	private final PropertiesUtil props;

	/**
	 * the sleepycat database environment
	 */
	private Environment environment;

	/**
	 * the sleepycat database
	 */
	private Database database = null;

	/**
	 * The "file" (it is actually the directory) the database is in
	 */
	private final File directory;

	/**
	 * the next collision id to assign
	 */
	private int minId;

	/**
	 * the collision list.
	 *
	 * This is keyed off of node identifiers to avoid running into problems with
	 * the GroupBy runId masking. By using NodeIdentifier, we end up doing the
	 * right thing since INodeData.nodeIdentifierEquals() delegates to the
	 * NodeIdentifier.equals().
	 *
	 * WARNING: Actually, not true.  INodeData.nodeIdentifierEquals() also
	 * checks the class of the INodeData, which NodeIdentifier doesn't do.  This
	 * is very dangerous, I've introduce checking in DiagnosticsSchema to ensure
	 * we never have a situation where the two equality checks are different.
	 *
	 * @see com.mercury.diagnostics.common.data.graph.DiagnosticsSchema#checkNodeDataTypeSchemaSafety
	 */
	private final Map<NodeIdentifier, Integer> collisionList;

	/** the symbol manager that is using this table, used to intern references in node datas */
	private final ISymbolManager symbolManager;

	/**
	 * Binding to convert INodeData to/from byte[]
	 */
	private TupleBinding NODE_DATA_BINDING = new TupleBinding() {

		public Object entryToObject(TupleInput input) {
			try {
				IWritable.DataIn.Impl in = new IWritable.DataIn.Impl(new DataInputStream(input), null, 0);

				in.setTokenizer(symbolManager);
				return WritableReader.read(in, null);
			}
			catch (ClassCastException cce) {
				return input;  // Caller might have a chance to fix this.
			}
			catch (IOException e) {
				//LH - this really shouldn't happen since we aren't actually doing any
				//     IO, just buffering data through (too many?) streams
				throw new RuntimeException("error deserializing node data.", e);
			}
		}

		public void objectToEntry(final Object object, TupleOutput output) {
			INodeData nodeData = (INodeData)object;

			try {
				IWritable.DataOut.Impl out = new IWritable.DataOut.Impl(
						new DataOutputStream(output), null, symbolManager);

				WritableReader.write(out, nodeData, "nodedata");
			}
			catch (IOException e) {
				//LH - this really shouldn't happen since we aren't actually doing any
				//     IO, just buffering data through (too many?) streams
				throw new RuntimeException("error serializing node data: " + nodeData + ".", e);
			}
		}
	};

	/** toggle to enable fixing of corrupt symbols (e.g. ProbeData with corrupt HostData) */
	private boolean repairCorruptSymbols = false;

	private Database collisionsDB;

	/**
	 * Pico constructor for creating the local symbol table
	 *
	 * Requiring the ByteBufferConfig ensures that it has been initialized
	 * prior to us being constructed.
	 */
	public LocalSymbolTable(PropertiesUtil props, IStorageManager storageManager, ISymbolManager symbolManager,
			TimeManager timeManager, ByteBufferConfig bbc) {

		this(props, storageManager.getSymbolDir(), symbolManager, timeManager);
	}

	/**
	 * Constructor for creating the local symbol table given the symbol directory
	 * instead of symbol manager.
	 */
	protected LocalSymbolTable(File symbolDirectory, ISymbolManager symbolManager, TimeManager timeManager) {
		this(new PropertiesUtil(), symbolDirectory, symbolManager, timeManager);
	}

	/**
	 * Constructor for creating the local symbol table given the symbol directory instead of symbol manager.
	 */
	private LocalSymbolTable(PropertiesUtil props, File symbolDirectory, ISymbolManager symbolManager, TimeManager timeManager) {

		this.props = props;
		this.symbolManager = symbolManager;
		this.timeManager = timeManager;
		this.directory = symbolDirectory;
		this.collisionList = new HashMap<NodeIdentifier, Integer>();
		this.minId = ApHash.MIN_ID_SEED;
		this.repairCorruptSymbols = props.getProperty("symboltable.repair.corrupt.symbols", false);
	}

	/**
	 * The initialization sequence of the symbol table. If <code>allowCreate</code>
	 * is false, DatabaseNotFoundException will be throw when the database is not found.
	 */
	public void initializeSymbolTable(boolean allowCreate) throws DatabaseException {
		try {
			environment = openEnvironment(directory);

			openDatabase(allowCreate);

			boolean shouldReinititalizeCollisionsList = props.getProperty("symboltable.reinitialize.collisions.list.on.startup", false);
			
			try {
				if (shouldReinititalizeCollisionsList) {
					environment.removeDatabase(null, COLLISIONS_DB);
				}
				
				Loggers.ARCHIVE.info("will now try to populate the collision list by using the collisions DB");
				
				DatabaseConfig dbConfig = new DatabaseConfig();
				dbConfig.setAllowCreate(false);
				collisionsDB = environment.openDatabase(null, COLLISIONS_DB, dbConfig);

				populateCollisionListFromCollisionsDB();
			} catch (DatabaseException e) {
				
				// we should only get here on the first run (when no collisions DB exists).
				
				Loggers.ARCHIVE.info("failed using collisions DB, will now try create the collisions DB");
				
				DatabaseConfig dbConfig = new DatabaseConfig();
				dbConfig.setAllowCreate(true);
				collisionsDB = environment.openDatabase(null, COLLISIONS_DB, dbConfig);
				
				populateCollisionList();

				// Sync the changes to disk
				environment.sync();
			} catch (IOException e) {
				Loggers.ARCHIVE.severe("got an IO exception while creating the collisions DB", e);
			}
		} catch (DatabaseException e) {
		  String errorStr = "Unable to initialize local symbol table\n";
      errorStr += "If you recently upgrade HP Diagnostics from 9.23 or earlier to 9.24 or later\n";
      errorStr += "You need to run sleepycat_upgrade.cmd(sh) under bin directory before starting the server.\n";      
      errorStr += "Please check Diagnostics_Upgrade_Patch_Install_Instructions.pdf for more details";
      
			Loggers.ARCHIVE.severe(errorStr, e);
			// Cleanup and rethrow
			close();
			throw e;
		}

		Loggers.ARCHIVE.info("The local symbol table contains " + database.count() + " symbols");
		Loggers.ARCHIVE.info("The collision list was created with " + collisionList.size() + " entries.");
		
		//add a shutdown hook if we got a database so we can close the DB properly
		if (null != database) {
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					close();
				}
			}));
		}
	}

	private void writeSingleCollisionToCollisionsDB(NodeIdentifier databaseEntryKey,
			Integer databaseEntryValue) {
		ByteArrayOutputStream keyOut = new ByteArrayOutputStream();
		IWritable.DataOut keyDataOut = new IWritable.DataOut.Impl(new DataOutputStream(keyOut), 
				new WritableTokenizer.Impl());

		try {
			databaseEntryKey.write(keyDataOut);
		} catch (IOException e) {
			Loggers.ARCHIVE.severe("could not convert nodeId to a byte array.", e);
		}

		DatabaseEntry key = new DatabaseEntry(keyOut.toByteArray());
		DatabaseEntry val = new DatabaseEntry();
		
		LSTKey.BINDING.objectToEntry(new LSTKey(databaseEntryValue), val);
		try {
			collisionsDB.put(null, key, val);
		} catch (DatabaseException e) {
			Loggers.ARCHIVE.severe("could not write a collision to the right DB.", e);
		}
	}

	private void populateCollisionListFromCollisionsDB()
			throws DatabaseException, IOException {
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry valueEntry = new DatabaseEntry();
		Cursor cursor = null;
		try {
			cursor = collisionsDB.openCursor(null, null);
	
			OperationStatus status = cursor.getNext(keyEntry, valueEntry, null);
			while (status == OperationStatus.SUCCESS) {
	
				NodeIdentifier key = getKeyFromDatabaseEntry(keyEntry);
				int value = getValueFromDatabaseEntry(valueEntry);
				updateCollisionTable(key,value);
				status = cursor.getNext(keyEntry, valueEntry, null);
			}
		} finally {
			if (cursor != null) {
				cursor.close();
			}
		}
	}

	/**
	 * gets the value of the collisions list entry from the Collisions DB entry.
	 */
	private int getValueFromDatabaseEntry(DatabaseEntry valueEntry) {
		LSTKey value = (LSTKey) LSTKey.BINDING.entryToObject(valueEntry);
		return value.localKey;
	}

	/**
	 * gets the key of the collisions list entry from the Collisions DB entry.
	 */
	private NodeIdentifier getKeyFromDatabaseEntry(DatabaseEntry keyEntry)
			throws IOException {
		byte[] keyData = keyEntry.getData();
		IWritable.DataIn keyDataIn = new IWritable.DataIn.Impl(new DataInputStream(
				new ByteArrayInputStream(keyData)), 0);
		// once we read this indentifir the dataIn cursor is no longer in location 0.
		long indentifier = keyDataIn.readLong();
		// this is called a second time in order to use it in the 
		// NodeIdentifier constructor, we need to have the cursor on location 0.
		keyDataIn = new IWritable.DataIn.Impl(new DataInputStream(
				new ByteArrayInputStream(keyData)), 0);

		NodeIdentifier key = new NodeIdentifier(indentifier, keyDataIn);
		return key;
	}

	/**
	 * When getting obj for key, sometimes we can't get an INodeData because it of corruption.
	 * (e.g. we've been getting ProbeDatas that get ClassCastException on their host).
	 * This routine will attempt to repair the "obj" if it isn't an INodeData.
	 *
	 * @param key - the database keyEntry
	 * @param obj - the iNodeData that we get back or the TupleInput when it is corrupt.
	 * @return the original INodeData if it is OK, the fixed one if we fixed it, else try to ClassCastException like before.
	 */
	private INodeData repairINodeData(DatabaseEntry key, Object obj) {
		INodeData nodeData;
		if (repairCorruptSymbols && obj instanceof TupleInput) {
			try {
				((TupleInput)obj).reset();
				IWritable.DataIn.Impl in = new IWritable.DataIn.Impl(new DataInputStream((TupleInput)obj), null, 0);
				in.setTokenizer(symbolManager);
				nodeData = (INodeData) WritableReader.readAndRepair(in, null);
				if (nodeData == null) {
					// Ummmm.  Well heck, just do it the old way again and I guess we'll get the old ClassCastException
					((TupleInput)obj).reset();
					in = new IWritable.DataIn.Impl(new DataInputStream((TupleInput)obj), null, 0);
					in.setTokenizer(symbolManager);
					nodeData = (INodeData)WritableReader.read(in, null );
				}
				else {
					// We did the readAndFix thing so let's set this to make the fix stick.
					LSTKey nodeDataKey = (LSTKey) LSTKey.BINDING.entryToObject(key);
					setValue(nodeDataKey.localKey, nodeData, true);
					Loggers.COMMON.severe("Repaired corrupt symbol " + nodeDataKey.localKey + ".  Set it to: " + nodeData);
				}
			} catch (IOException e) {
				throw new RuntimeException("error deserializing node data during readAndFix.", e);
			}
		}
		else {
			nodeData = (INodeData)obj;
		}
		return nodeData;
	}

	/**
	 * @param symbolManager may be null for UT
	 */
	public static LocalSymbolTable createUTInstance(ISymbolManager symbolManager)
			throws SymbolManagerInitializationException, DatabaseException {
		LocalSymbolTable ret;

		try {
			final File tmpDir = File.createTempFile("UT_LocalSymbolTable", ".dir");

			if (tmpDir.exists()) {
				tmpDir.delete();
			}
			tmpDir.mkdirs();

			ServerTimeManager timeManager = ServerTimeManager.createUTInstance(new PropertiesUtil());

			ret = new LocalSymbolTable(new PropertiesUtil(), new IStorageManager.Unsupported() {
				public File getSymbolDir() {
					return tmpDir;
				}
			}, symbolManager, timeManager, null);

			ret.initializeSymbolTable(true);
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
				public void run() {
					try {
						FileUtils.delete(tmpDir);
					}
					catch (IOException e) {
						Loggers.ARCHIVE.severe("failed to delete temporary local symbol table directory:" + tmpDir.toString());
					}
				}
			}
					));
		}
		catch (IOException ioe) {
			throw new SymbolManagerInitializationException(ioe);
		}
		return ret;
	}

	public void delete(NodeIdentifier dataKey) {
		//TODO remember to delete from the collision list as well (if needed)
		throw new UnsupportedOperationException("delete method is not implemented yet");
	}

	private void populateCollisionList() {
		ClosingIterator<INodeData> iter = null;

		int symbolsCount = 0; // for logging purpose only
		int skippedSymbolsCount = 0;
		int deletedSymbolsCount = 0;


		// If requested, delete symbols on startup
		boolean deleteSymbols = props.getProperty("symboltable.delete.symbols.on.startup", false);

		try {
			iter = new LSTIterator();
			while (iter.hasNext()) {

				INodeData data = null;
				try {
					data = iter.next();
				}
				catch (NoSuchElementException e) {
					Loggers.ARCHIVE.warning("Unable to de-serialize entity", e);
					if (deleteSymbols && ((LSTIterator)iter).delete()) {
						deletedSymbolsCount++;
					} else {
						skippedSymbolsCount++;
					}
					continue;
				}
				catch (ClassCastException e) {
					Loggers.ARCHIVE.warning("Unable to de-serialize entity", e);
					if (deleteSymbols && ((LSTIterator)iter).delete()) {
						deletedSymbolsCount++;
					} else {
						skippedSymbolsCount++;
					}
					continue;
				}

				symbolsCount++;

				// LSTIterator will make sure that the symbol token is set
				// properly on the data's identifier.
				int localKey = data.getNodeIdentifier().getSymbolToken();

				updateCollisionTable(data.getNodeIdentifier() ,localKey);
			}

			Loggers.ARCHIVE.info("Scanned " + symbolsCount + " records from local symbol table (skipped "+skippedSymbolsCount+")");
			if (deleteSymbols) {
				Loggers.ARCHIVE.info("Deleted " + deletedSymbolsCount + " records from local symbol table");
				if (deletedSymbolsCount > 0) {
					environment.sync();  // write the changes.
				}
			}

			if (collisionList.size() > 0) {
				Loggers.ARCHIVE.info("Found and resolved " + collisionList.size() + " symbol collisions.");
			}
		}
		catch (DatabaseException e) {
			Loggers.ARCHIVE.severe("error scanning local symbol table for collisions");
		}
		finally {
			if (null != iter) {
				iter.close();
			}
		}
	}

	private void updateCollisionTable(NodeIdentifier data, int localKey) {
		if (localKey < ApHash.MIN_ID_SEED) {
			collisionList.put(data, localKey);
			writeSingleCollisionToCollisionsDB(data, new Integer(localKey));
		}

		// This is too late to safely increment the minId.  Suppose the put failed with OOM, but server continued on
		// and later attempts used the db entry, set it (for example host local key set in probeData) and then
		// some RemoteMethodData came along later and re-used its collision Id?
		// ...
		// So I changed it to decrement the minId as soon as it is claimed, but let's keep this old check/decrement
		// just in case.
		if (localKey < minId) {
			minId = localKey;
		}
	}

	private Environment openEnvironment(File symbolDir) throws DatabaseException {
		// Must 'sanitize' properties to only include Sleepycat specific ones, since
		// EnvironmentConfig does not like seeing entries it does not know about.
		Properties envProps = props.getPropertiesForPrefix("sleepycat.");

		// Log the config entries so we know what was used
		for (Map.Entry<Object,Object> envPropEntry : envProps.entrySet()) {
			Loggers.ARCHIVE.info("[Berkeley DB] " + envPropEntry.getKey() + " = " + envPropEntry.getValue());
		}

		// Create the environment
		EnvironmentConfig envConfig = new EnvironmentConfig(envProps);

		envConfig.setAllowCreate(true);

		Environment env = new Environment(symbolDir, envConfig);

		// If requested, clean the log files
		if (props.getProperty("symboltable.run.cleaner.on.startup", true)) {
			int logsCleaned = env.cleanLog();

			if (logsCleaned > 0) {
				env.checkpoint(new CheckpointConfig());
				Loggers.ARCHIVE.info("Cleaned " + logsCleaned + " database log " + (logsCleaned == 1 ? "file." : "files."));
			}
		}

		return env;
	}

	private void openDatabase(boolean allowCreate) throws DatabaseException {

		DatabaseConfig dbConfig = new DatabaseConfig();

		dbConfig.setAllowCreate(false);

		try {
			//LH - this will fail so we can detect when we need to migrate the
			//     old symbol table to sleepycat
			openDatabaseAndValidate(dbConfig);
		}
		catch (DatabaseNotFoundException dnfe) {
			// If the caller does not want to allow database creation, then simply rethrow
			if (false == allowCreate) {
				throw dnfe;
			}

			dbConfig.setAllowCreate(true);

			openDatabaseAndValidate(dbConfig);

			upgrade();

			// Sync the changes to disk
			environment.sync();
		}
	}

	/**
	 * Opens the database and sets this instance's "database" field.  Logs a
	 * severe message if "database" is set to null, because the rest of the class
	 * expects "database" to be non-null.
	 * @param dbConfig
	 * @throws com.sleepycat.je.DatabaseException
	 */
	private final void openDatabaseAndValidate(final DatabaseConfig dbConfig)
			throws DatabaseException {
		database = environment.openDatabase(null, DATABASE_NAME, dbConfig);
		if ( database == null ) {
			Loggers.ARCHIVE.severe("Unable to open DATABASE_NAME '"
					+ DATABASE_NAME + "' from dbConfig -- subsequent code may not"
					+ " function properly...",
					new NullPointerException("'LocalSymbolTable.database' is null!"));
		}
	}

	public synchronized void close() {
		if (database != null) {
			try {
				database.close();
			}
			catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("error closing database: " + e);
			}
			finally {
				database = null;
			}
		}
		
		if (collisionsDB != null) {
			try {
				collisionsDB.close();
			}
			catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("error closing database: " + e);
			}
			finally {
				collisionsDB = null;
			}
		}

		if (null != environment) {
			try {
				environment.close();
			}
			catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("error closing local symbol table database environment after error creating database: " + e);
			}
			finally {
				environment = null;
			}
		}
	}

	public INodeData getValue(int localKey) {
		if ( databaseIsNotNull() ) {
			try {
				LSTKey key = new LSTKey(localKey);

				DatabaseEntry keyEntry = new DatabaseEntry();
				DatabaseEntry valueEntry = new DatabaseEntry();

				LSTKey.BINDING.objectToEntry(key, keyEntry);

				OperationStatus status = database.get(null, keyEntry, valueEntry, null);

				if (OperationStatus.SUCCESS == status) {
					// Get the INodeData and attempt to repair corrupt ProbeData, if needed.
					INodeData ret = repairINodeData(keyEntry, NODE_DATA_BINDING.entryToObject(valueEntry));
					ret.getNodeIdentifier().setSymbolToken(localKey);
					return ret;
				}
			}
			catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("error looking up data in local symbol table", e);
				throw new RuntimeException("error getting data for the specified key: " + localKey);
			}
		}
		return null;
	}



	public void setValue(int localKey, INodeData data) {
		setValue(localKey, data, true);
	}



	/**
	 * The actual setValue, also allows the sync to be skipped for the upgrade process
	 * @param localKey
	 * @param data
	 * @param forceSync
	 */
	public void setValue(int localKey, INodeData data, boolean forceSync) {

		if (Loggers.SYMBOLS_DEBUG.isEnabledFor(Level.DEBUG)) {
			Loggers.SYMBOLS_DEBUG.debug("setValue " + localKey + ": " + data);
		}

		if (data.supportsNII()) {
			//LH - not formated into a date since we can't do that properly outside ui
			data.setInfo(NodeDataFactory.UNSPECIFIED_PATH_KEY,
					NIIKey.last_modified.toString(),
					Long.toString(timeManager.getCurrentTargetTime()));

			data.incrementRevision();
		}

		LSTKey key = new LSTKey(localKey);

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry valueEntry = new DatabaseEntry();

		LSTKey.BINDING.objectToEntry(key, keyEntry);
		NODE_DATA_BINDING.objectToEntry(data, valueEntry);

		try {
			if ( databaseIsNotNull() ) {
				database.put(null, keyEntry, valueEntry);
			}

			updateCollisionTable(data.getNodeIdentifier(), localKey);

			// Sync causes any dirty entries in the in-memory cache and the operating system's
			// file cache to be written to disk. As such, sync is quite expensive and should
			// be used sparingly. Here we don't have a choice. We must ensure the data makes
			// it to the disk even if the server crashes or gets shutdown from nanny (which
			// is almost like a crash).
			if (forceSync) { // for faster upgrades
				environment.sync();
			}
		}
		catch (DatabaseException e) {
			throw new RuntimeException("error adding symbol to local symbol table", e);
		}
	}



	public ClosingIterator<INodeData> getSymbolTableIterator() {
		try {
			return new LSTIterator();
		}
		catch (DatabaseException e) {
			throw new RuntimeException(e);
		}
	}



	/**
	 * Min Id is a running negative integer that is kept for resolving collisions
	 * This returns the next number to use and increments minId so the next caller won't get the same one.
	 * @return the minId - next collision value to use.
	 */
	synchronized public int takeNextMinId() {
		return --minId;
	}



	public synchronized int getFromCollisionList(INodeData data) {
		// Uh.  NodeIdentifiers don't identify type.  This means that
		// two entities with the same NI fields set are the same object...
		// which will *definitely* not work if it happens.  Run a schema
		// validation to see what cases we have where two different
		// types of INodeData have == identifiers, because this is
		// the case we have to worry about (and defend with just by
		// changing the identifier even if we don't change the hashvalue?)
		//
		// Might be the source of some of our collision problems...
		Integer integer = collisionList.get(data.getNodeIdentifier());

		if (null != integer) {
			return integer.intValue();
		}
		else {
			return ISymbolManager.UNSPECIFIED_LOCAL_KEY;
		}
	}

	/**
	 * A key in the local symbol table database
	 */
	private static class LSTKey {
		private final int localKey;

		/**
		 * binding object for LSTKey<->byte[]
		 */
		public static final EntryBinding BINDING = new TupleBinding() {

			public LSTKey entryToObject(TupleInput input) {
				int localKey = input.readInt();

				return new LSTKey(localKey);
			}



			public void objectToEntry(Object object, TupleOutput output) {
				LSTKey key = (LSTKey)object;

				output.writeInt(key.localKey);
			}
		};

		LSTKey(int localKey) {
			this.localKey = localKey;
		}

	}




	/**
	 * Iterator that traverses the local symbol table and returns the node datas in it.
	 */
	private class LSTIterator extends ClosingIterator<INodeData> {

		private final Cursor cursor;
		private final DatabaseEntry key;
		private final DatabaseEntry value;

		private boolean moveToNext;
		private OperationStatus iterStatus;

		public LSTIterator() throws DatabaseException {
			cursor = databaseIsNotNull() ? database.openCursor(null, null) : null;

			this.key = new DatabaseEntry();
			this.value = new DatabaseEntry();
			moveToNext = true;
		}



		public boolean hasNext() {
			moveToNext();
			return (iterStatus == OperationStatus.SUCCESS);
		}



		public INodeData next() {
			moveToNext();
			if (OperationStatus.SUCCESS == iterStatus) {
				moveToNext = true;

				// Node data we are going to return  (with attempt to repair corrupt ProbeData if needed)
				INodeData nodeData = repairINodeData(key, NODE_DATA_BINDING.entryToObject(value));


				// Node identifier holds the symbol token.
				NodeIdentifier nodeIdentifier = nodeData.getNodeIdentifier();

				// The symbolToken field currently is not serialized (IWritable). Therefore,
				// nodeIdentifier.getSymbolToken() in reality always returns 0 below,
				// but I have included the 'if' check just in case we will serialize it
				// at some point. The main idea here is that before we return the node data,
				// we make sure that it has a symbol token assigned. Without it, the collision
				// table was not properly initialized on server restarts causing wrong
				// symbol assignments.
				if (NodeDataFactory.UNSPECIFIED_LOCAL_KEY == nodeIdentifier.getSymbolToken()) {

					// This has the actual symbol token that was assigned to the node data.
					LSTKey nodeDataKey = (LSTKey)LSTKey.BINDING.entryToObject(key);

					// So use it :)
					nodeIdentifier.setSymbolToken(nodeDataKey.localKey);
				}

				return nodeData;
			}
			else {
				throw new NoSuchElementException();
			}
		}


		private void moveToNext() {
			if (moveToNext) {
				try {
					iterStatus = cursor != null ? cursor.getNext(key, value, null)
							: OperationStatus.NOTFOUND;
					moveToNext = false;
				}
				catch (DatabaseException e) {
					close();
					throw new RuntimeException("error getting local symbol table iterator", e);
				}
			}
		}


		/**
		 * Don't remove this if it looks un-used.  I might want to access this from bsh.
		 * @return bool
		 */
		public boolean delete() {
			try {
				return cursor.delete() == OperationStatus.SUCCESS;
			} catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("LSTIterator delete() exception: " + e);
				return false;
			}
		}



		public void close() {
			super.close();
			try {
				if ( cursor != null ) {
					cursor.close();
				}
			}
			catch (DatabaseException e) {
				Loggers.ARCHIVE.severe("error closing local symbol table iterator", e);
			}
		}
	}

	/**
	 * UT method, do not expose on interface since a LST implementation may NOT have a directory
	 */
	public File getDirectory() {
		return directory;
	}



	/**
	 * upgrade the symbol table into the specified database
	 */
	private synchronized void upgrade() {
		//get the old symbol table file
		File oldSymbolTable = new File(directory, "SymbolTable.db");
		if (oldSymbolTable.exists()) {
			Loggers.ARCHIVE.info("upgrading local symbol table");
			SymbolFile symbolFile = null;
			ClosingIterator<INodeData> iter = null;

			try {
				symbolFile = new SymbolFile(oldSymbolTable.getPath());
				iter = symbolFile.getSymbolTableIterator();
				while (iter.hasNext()) {
					INodeData data = iter.next();

					setValue(data.getNodeIdentifier().getSymbolToken(), data, false);
				}
			}
			catch (SymbolManagerInitializationException e) {
				Loggers.ARCHIVE.severe("error upgrading local symbol table", e);
			}
			finally {
				if (null != symbolFile) {
					symbolFile.close();

					//LH - don't delete the file since it may be handy to have around in
					//     case we have upgrade issues and need to redo the upgrade. It
					//     won't cause problems since after we do this, our database has
					//     been created and we won't try to do the upgrade.

				}

				if (null != iter) {
					iter.close();
				}
				Loggers.ARCHIVE.info("Upgrading of Symbol Table Complete");
			}
		}
	}



	/**
	 * Centralizes the check to see if the "database" field is null, when it is
	 * expected to be non-null.
	 * Returns 'true' if this instance's "database" field is not null.
	 * Logs a warning and returns 'false' if the "database" field is null.
	 * @returns 'true' if this instance's "database" field is not null.
	 */
	private final boolean databaseIsNotNull() {
		if ( database != null ) {
			return true;
		}
		Loggers.ARCHIVE.warning("The 'database' field is unexpectedly null --"
				+ " taking steps to mitigate the problem.",
				new NullPointerException("'LocalSymbolTable.database' is null!"));
		return false;
	}
}
