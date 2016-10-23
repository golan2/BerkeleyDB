/*
 * (c) Copyright 2008 Hewlett-Packard Development Company, L.P. 
 * --------------------------------------------------------------------------
 * This file contains proprietary trade secrets of Hewlett Packard Company.
 * No part of this file may be reproduced or transmitted in any form or by
 * any means, electronic or mechanical, including photocopying and
 * recording, for any purpose without the expressed written permission of
 * Hewlett Packard Company.
 */


package com.mercury.diagnostics.server.persistence.impl;


import com.mercury.diagnostics.common.data.graph.*;
import com.mercury.diagnostics.common.data.graph.impl_oo.AggregateTreeNode;
import com.mercury.diagnostics.common.data.graph.impl_oo.InstanceTreeNode;
import com.mercury.diagnostics.common.data.graph.impl_oo.StoredTreeRecord;
import com.mercury.diagnostics.common.data.graph.impl_oo.WritableReader;
import com.mercury.diagnostics.common.data.graph.node.*;
import com.mercury.diagnostics.common.io.ByteBufferConfig;
import com.mercury.diagnostics.common.io.INodeData;
import com.mercury.diagnostics.common.io.NodeDataFactory;
import com.mercury.diagnostics.common.io.NodeIdentifier;
import com.mercury.diagnostics.common.io.externalstorage.Block;
import com.mercury.diagnostics.common.io.externalstorage.FileStorageMedium;
import com.mercury.diagnostics.common.io.externalstorage.InputBuffer;
import com.mercury.diagnostics.common.modules.monitoring.LongCounterMetric;
import com.mercury.diagnostics.common.modules.monitoring.Monitor;
import com.mercury.diagnostics.common.modules.monitoring.MonitorModule;
import com.mercury.diagnostics.common.util.*;
import com.mercury.diagnostics.common.util.collections.Pair;
import com.mercury.diagnostics.server.persistence.IStorageManager.Suffix;
import com.mercury.diagnostics.server.persistence.IStorageManager;
import com.mercury.diagnostics.server.persistence.MajorDuration;
import com.mercury.diagnostics.server.persistence.symboltable.impl.ApHash;
import com.mercury.diagnostics.server.persistence.symboltable.impl.ClosingIterator;
import com.mercury.diagnostics.server.persistence.symboltable.impl.IntLongIntHashIterator;
import com.mercury.diagnostics.server.persistence.symboltable.impl.LocalSymbolTable;
import com.mercury.diagnostics.server.persistence.symboltable.impl.SymbolManager;
import com.mercury.diagnostics.server.time.ServerTimeManager;
import com.mercury.opal.mediator.util.DiagnosticsServer;

import EDU.oswego.cs.dl.util.concurrent.Executor;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.ThreadFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;


/** A utility for persistence to dump persistence files (as well as modify summaries). See usage for more details. */
public class PersistenceDumper {

  private static final String DELIMITER = "#";
  public final Persistence persistence;

  private static PersistenceDumper dumper;

  private static DiagProperties properties;

  private boolean outputToStdOut = true;
  boolean dumpHeaders = true;
  private boolean quit = false;
  private boolean rewriteSummaries = false;
  private ArrayList<FieldCondition> summaryConditions = new ArrayList<FieldCondition>();
  private ArrayList<FieldModificationCondition> rewriteConditions = new ArrayList<FieldModificationCondition>();
  private ArrayList<EntityCondition> entityConditions = new ArrayList<EntityCondition>();
  private boolean autoRotate = false;
  private boolean autoFix = false;
  private String deletionBatchFile = null;
  private PrintStream deletionBatchOptions = null;
  private boolean summaryStats = false;
  private boolean pathedFix = false;

  /**
   * Maps old pathed key to a new pathed key (w/generation).
   */
  private final Map<Long, Long> oldToNewMapping = new HashMap<Long, Long>();

  /**
   * The server instance -- the static initializers for this class are an important side effect for setting mediator.io
   * -- so leave this here :)
   */
  final DiagnosticsServer server;

  private static final String           INDENT_CHAR = "  "; // or use "\t";
  private static final SimpleDateFormat SDF         = new SimpleDateFormat("yyyy-MM-dd");

  static final String SINGLE_SYMBOL       = "single_symbol";
  static final String SYMBOLS             = "symbols";
  static final String DUMP_SYMBOL_OF_TYPE = "dumpSymbolOfType";
  static final String SYMBOLSSTATS        = "symbolStats";
  static final String PSTSYMBOLSSTATS     = "pstSymbolStats";
  static final String PATHED              = "pathed";
  static final String PATHED_TREE         = "pathedTree";
  static final String PATHED_STATS        = "pathedStats";
  static final String RESETDB             = "resetdb";
  static final String CHILDREN            = "children";
  static final String SUMMARIES           = "summaries";
  static final String REWRITESUMMARIES    = "rewritesummaries";
  static final String VALIDATEDB          = "validatedb";
  static final String QUIT                = "quit";
  static final String EXPORTDB            = "exportToDB";
  static final String MERGEDPATHED        = "mergepathed";
  static final String REWRITEPATHEDKEYS   = "rewritepathkeys";
  static final String DELETEORPHANS       = "deleteorphanednodes";
  static final String GCTEST              = "gctest";
  static final String COUNTSR             = "countsr";
  static final String DELETEPATHKEY       = "deletepk";
  static final String CHECKARCHIVE        = "checkarchive";
  static final String HOTSPOTS            = "h";


  public String dbTablePrefix    = null;
  public String dbAltTablePrefix = null;
  public String backupSymbolDir  = null;
  String dbOracleURL      = null;
  String dbOracleUser     = null;
  String dbOraclePassword = null;


  static class Action {
    String name;
    String desc;
    boolean requiresDb;

    Action(String name, String desc, boolean requiresDb) {
      this.name = name;
      this.desc = desc;
      this.requiresDb = requiresDb;
    }
  }

  static final Action[] ACTIONS = new Action[] {
      new Action(SYMBOLS, "Output Local Symbol Table (LST)", false),
      new Action(SINGLE_SYMBOL, "Output INodeData according to PK", false),
      new Action(PATHED, "Output Pathed Symbol Table (PST)", false),
      new Action(PATHED_TREE, "Output pathed symbol table as a tree", false),
      new Action(PATHED_STATS, "Statistics about the Pathed Symbol Table (PST)", false),
      new Action(RESETDB, "Recreate the DB (drop and create tables)", true),
      new Action(CHILDREN, "Dump children of a particular node", false),
      new Action(SUMMARIES, "Dump summaries", false),
      new Action(REWRITESUMMARIES, "Rewrite a set of summary files making some changes", false),
      new Action(VALIDATEDB, "Validate the DB schema", true),
      new Action(EXPORTDB, "Export to the DB", true),
      new Action(GCTEST, "Find garbage (local symbols no longer referenced in the path symbol table)", false),
      new Action(COUNTSR, "Report number of Server Requests per Probe", false),
      new Action(DELETEPATHKEY, "Delete an entity via its pathkey", false),
      new Action(DELETEORPHANS, "Delete orphaned nodes from PST", false),
      new Action(CHECKARCHIVE, "Check archive", false),
      new Action(SYMBOLSSTATS, "Report on breakdown of types in symbol table", false),
      new Action(PSTSYMBOLSSTATS, "Report on breakdown of types in pathed symbol table", false),

      // new Action(HOTSPOTS, "Experimental: read tree files", false)
  };

  //  private PrintStream lhfile = new PrintStream(new FileOutputStream("/tmp/lhfile", false));

  private static class MyProp extends DiagProperties {

    public MyProp(File file) {
      super(file);
    }

    //@Override
    //public String getProperty(String key, String defaultValue) {
    //  String value = super.getProperty(key, defaultValue);
    //  System.out.println("["+key+"=["+value+"]");
    //  return value;
    //}
    //
    //@Override
    //public String getProperty(String key) {
    //  String value = super.getProperty(key);
    //  System.out.println("["+key+"=["+value+"]");
    //  return value;
    //}

    //@Override
    //public String get(String key) {
    //  String value = this.getProperty_nolog(key);
    //  System.out.println("["+key+"=["+value+"]");
    //  return value;
    //}

    @Override
    public String getProperty(String key) {
      String value = super.getProperty(key);
      System.out.println("["+key+"=["+value+"]");
      return value;
    }



  }

  public static void main(String[] args) throws Exception {

    properties = new MyProp(new File("C:\\Users\\golaniz\\Desktop\\Misc\\QCIM1I108840_LLoyds\\server.properties"));
    properties.load();


    ByteBufferConfig.initialize(properties, new NullLogger());

    dumper = new PersistenceDumper(properties);
    dumper.readCommands();
  }



  private static void dumpLocalSymbolTable(final File dir, PropertiesUtil properties) {

    ServerTimeManager timeManager = ServerTimeManager.createUTInstance(new PropertiesUtil());

    LocalSymbolTable lst = new LocalSymbolTable(properties, new IStorageManager.Unsupported() {
      public File getSymbolDir() {
        return dir;
      }
    }, null, timeManager, null);

    try {
      lst.initializeSymbolTable(false);

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }



  private void readCommands() {

    while (!quit) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

      System.out.print("dump>");
      PrintStream outputOptions = null;

      preCommandCleanup();

      try {
        Thread.sleep(200);

        String in = reader.readLine();
        String[] commandArgs = in.split("\\s");

        outputOptions = getOutputOptions(commandArgs);
        dumpHeaders = shouldDumpHeaders(commandArgs);
        for (int i = 0; i < commandArgs.length; i++) {
          if (processCommand(outputOptions, commandArgs, i)) {
            break;
          }
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }
      finally {
        if (outputOptions != null && !outputToStdOut) {
          outputOptions.flush();
          outputOptions.close();
        }
        postCommandCleanup();
      }
      System.out.println();
    }

  }



  void postCommandCleanup() {
    if (deletionBatchOptions != null) {
      deletionBatchOptions.flush();
      deletionBatchOptions.close();
    }
  }



  void preCommandCleanup() {
    deletionBatchOptions = null;
    rewriteSummaries = false;
    autoRotate = false;
    autoFix = false;
    deletionBatchFile = null;
  }



  boolean processCommand(PrintStream outputOptions, String[] commandArgs, int i) throws Exception {
    if (commandArgs[i].equalsIgnoreCase(SYMBOLS)) {
      getDbOptions(commandArgs);
      dumpSymbolTable(outputOptions);
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(DUMP_SYMBOL_OF_TYPE)) {
      dumpSymbolOfType(outputOptions);
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(SINGLE_SYMBOL)) {
      getDbOptions(commandArgs);
      dumpSingleSymbol(outputOptions, getSingleSymbolEntityOptions(commandArgs));
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(PATHED)) {
      getDbOptions(commandArgs);
      dumpPathedSymbolTable(outputOptions);
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(PATHED_TREE)) {
      dumpPathedSymbolTableTree(outputOptions);
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(PATHED_STATS)) {
      dumpPathedSymbolTableStats(outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(RESETDB)) {
      getDbOptions(commandArgs);
      Exporter e = new Exporter(this.dbOracleURL, this.dbOracleUser, this.dbOraclePassword, this.dbTablePrefix);

      e.setupDefaultSchema();
      e.close();
    }
    else if (commandArgs[i].equalsIgnoreCase(CHILDREN)) {
      dumpChildren(commandArgs, outputOptions);
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(SUMMARIES)) {
      getDbOptions(commandArgs);
      getEntityConditions(commandArgs);
      getFieldConditions(commandArgs);
      getStatOutput(commandArgs);
      dumpSummaries(outputOptions, getCustomerOptions(commandArgs), getDurationOptions(commandArgs), getSpecificFileOptions(commandArgs));
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(REWRITESUMMARIES)) {
      rewriteSummaries = true;
      getEntityConditions(commandArgs);
      getFieldModifications(commandArgs);
      autoRotate = getAutoRotate(commandArgs);
      autoFix = getAutoFix(commandArgs);
      dumpSummaries(outputOptions, getCustomerOptions(commandArgs), getDurationOptions(commandArgs), getSpecificFileOptions(commandArgs));
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(VALIDATEDB)) {
      getDbOptions(commandArgs);
      validateDb(outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(EXPORTDB)) {
      getDbOptions(commandArgs);
      exportDb(commandArgs, outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(MERGEDPATHED)) {
      mergePathedSymbolTable(outputOptions, getSpecificFileOptions(commandArgs));
    }
    else if (commandArgs[i].equalsIgnoreCase(REWRITEPATHEDKEYS)) {
      rewriteSummaries = true;
      pathedFix = true;
      oldToNewMapping.clear();
      autoRotate = getAutoRotate(commandArgs);
      rewritePathkeys(outputOptions, getCustomerOptions(commandArgs), getDurationOptions(commandArgs), getSpecificFileOptions(commandArgs));

    }
    else if (commandArgs[i].equalsIgnoreCase(DELETEORPHANS)) {
      deleteOrphanedNodes(outputOptions);
      quit = true;

    }
    else if (commandArgs[i].equalsIgnoreCase(QUIT)) {
      quit = true;
      return true;
    }
    else if (commandArgs[i].equalsIgnoreCase(GCTEST)) {
      gcTest(outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(COUNTSR)) {
      countSRs(outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(SYMBOLSSTATS)) {

      countSymbolTableBreakdown(outputOptions);
    }
    else if (commandArgs[i].equalsIgnoreCase(PSTSYMBOLSSTATS)) {
      countPSTSymbolsByType(outputOptions);
    }

    else if (commandArgs[i].equalsIgnoreCase(DELETEPATHKEY)) {
      deleteEntity(outputOptions, getDeleteEntityOptions(commandArgs));
    }
    else if (commandArgs[i].equalsIgnoreCase(CHECKARCHIVE)) {
      checkArchive(outputOptions, commandArgs);
    }
    else if (commandArgs[i].equalsIgnoreCase(HOTSPOTS)) {
      hotspots(outputOptions, commandArgs);
    }
    else {
      outputOptions.println("Usage:");
      outputOptions.println("<command> [-o outputfile] [-noheaders] <summaryoptions> <mergepathedoptions> <rewritepathkeysoptions> <deletepkoptions>");
      outputOptions.println("<command> is one of the:");
      outputOptions.println("    symbols - Dumps the local symbol table");
      outputOptions.println("    pathed - Dumps the pathed symbol table");
      outputOptions.println("    summaries - Dumps the summary files");
      outputOptions.println("    symbolStats - Report on breakdown of types in symbol table");
      outputOptions.println("    pstSymbolStats - Report on breakdown of types in pathed symbol table");
      outputOptions.println("    rewritesummaries - Modifies summaries with modification conditions");
      outputOptions.println("    mergepathed - Merges a pathed symbol file (using -f) into the current excluding pk with new genIds");
      outputOptions.println("    deletepk - Delete a pathkey from the pathed symbol table");
      outputOptions.println("    deleteorphanednodes - Remove orphaned nodes from the pathed symbol table");
      outputOptions.println("    rewritepathkeys - Given a pathed symbol file, rewrite pathed keys with new genId in summary files");
      outputOptions.println("    gctest - Experimental");
      outputOptions.println("    checkarchive - Checks consistency of summary files. Use [rename] option to rename inconsistent summary files");
      outputOptions.println("    quit - Exit the dumper utility");
      outputOptions.println("-noheaders - Don't print the column headers for CSV format");
      outputOptions.println("<summaryoptions> are:");
      outputOptions.println("    [-d major,minor | -d major][-d ...] [-cn customername] [-f summaryfilename][-f ...] [<fieldcondition>][<fieldcondition][...] [<modification condition>][...] [<entitycondition>][...]");
      outputOptions.println("<fieldcondtions> are: ");
      outputOptions.println("    -fc value|count|min|max|sumsq|exc|tout eq|lt|gt|ne VALUE");
      outputOptions.println("    where VALUE is any number. When Field Conditions are applied to rewritesummaries command, ");
      outputOptions.println("    the resulting set is not reduced, and the condition is actually applied to the modification itself; ");
      outputOptions.println("    i.e. the value in summary is going to be modified only if one of the field conditions is met (unless no field conditions are specified) ");
      outputOptions.println("<entitycondtions> are: ");
      outputOptions.println("    -ec dataclass|pathkey|localkey eq|ne VALUE");
      outputOptions.println("    where VALUE is: ");
      outputOptions.println("         A class name of INodeData in case of dataclass (MethodData, FragmentData etc...) ");
      outputOptions.println("         A key in case of pathkey or localkey ");
      outputOptions.println("    When Entity Conditions are applied to rewritesummaries command, ");
      outputOptions.println("    the resulting set is not reduced, and the condition is actually applied to the modification itself; ");
      outputOptions.println("    i.e. the value in summary is going to be modified only if one of the entity conditions is met (unless no entity conditions are specified) ");
      outputOptions.println("<modification conditions> are: ");
      outputOptions.println("    -mc value|count|min|max|sumsq|exc|tout eq|lt|gt|ne VALUE NEWVALUE");
      outputOptions.println("    where VALUE is any number and NEWVALUE is the value to be written instead of old value if condition is met");
      outputOptions.println("    The rewriting will create .dumper files which will have to be manually rotated later on");
      outputOptions.println("    You can use a simple shell script like the following");
      outputOptions.println("      for i in `ls *.dumper`; do mv $i `echo $i | cut -d . -f 1`.summary; done");
      outputOptions.println("    to move the .dumper files to the actual summary files");
      outputOptions.println("<mergepathedoptions> are: ");
      outputOptions.println("    [-f sourcepathedfilame]");
      outputOptions.println("    -f file name of the source pathed symbol file.");
      outputOptions.println("<rewritepathkeysoptions> are: ");
      outputOptions.println("    [-f sourcepathedfilame] [-d major,minor | -d major][-d ...] [-cn customername] ");
      outputOptions.println("    -f file name of the source pathed symbol file.");
      outputOptions.println("    -d major,minor summaries to rewrite.");
      outputOptions.println("    -cn customer name");
      outputOptions.println("<deletepkoptions> are: ");
      outputOptions.println("    -pk pathkey");
      outputOptions.println("NOTE: [-o outputfile] will overwrite outputfile if it exists");

    }
    return false;
  }

  private long getSingleSymbolEntityOptions(String[] commandArgs) {
    return getPkFromCommandArgs(commandArgs);
  }

  private void exportDb(String[] commandArgs, PrintStream outputOptions) throws Exception {
    Exporter e = new Exporter(this.dbOracleURL, this.dbOracleUser, this.dbOraclePassword, this.dbTablePrefix);

    e.setupDefaultSchema();
    e.close();

    dumpSymbolTable(outputOptions);
    dumpPathedSymbolTable(outputOptions);
    dumpSummaries(outputOptions, getCustomerOptions(commandArgs), getDurationOptions(commandArgs), getSpecificFileOptions(commandArgs));
  }



  /**
   * If you've exported everything to a database, this function validates the DB.
   *
   * @param outputOptions
   * @throws Exception
   */
  private void validateDb(PrintStream outputOptions) throws Exception {
    Exporter e = new Exporter(this.dbOracleURL, this.dbOracleUser, this.dbOraclePassword, this.dbTablePrefix);

    try {
      //
      //  Undefined Local Keys
      //
      outputOptions.print("Undef LK in PST: ");
      ResultSet rs = e.stmt.executeQuery("select count(*) from PST where LK not in (select LK from LST)");

      rs.next();
      int c = rs.getInt(1);

      if (c != 0) {
        outputOptions.println("ERR - " + c + " LK's in PST that aren't defined in LST");
      }
      else {
        outputOptions.println("OK");
      }

      //
      //  Undefined Parent PK's
      //
      outputOptions.print("Undef PARENTPK in PST: ");
      rs = e.stmt.executeQuery("select count(*) from PST where PARENTPK not in (select PK from PST)");
      rs.next();
      c = rs.getInt(1);
      if (c != 0) {
        outputOptions.println("ERR - " + c + " PARENTPK's in PST that aren't defined in PST");
      }
      else {
        outputOptions.println("OK");
      }

      //
      //  Differing definitions of path keys
      //
      outputOptions.print("Differing PK definitions: ");
      rs = e.stmt.executeQuery("select count(*) from (\n" +
          "            select PK, count(pk) as Num from (\n" +
          "              select PK, LK, PARENTPK, count(pk) as Num FROM PST GROUP BY PK,LK,PARENTPK HAVING (COUNT(pk)>1)\n" +
          "            ) GROUP BY PK HAVING (COUNT(pk)>1)\n" +
          "          )");
      rs.next();
      c = rs.getInt(1);
      if (c != 0) {
        outputOptions.println("ERR - " + c + " PK's with differing definitions");
      }
      else {
        outputOptions.println("OK");
      }

      /*

       Here are some example SQL queries you can run after exporting the TSDB to a SQL database

       --- Output top-level PST entries -------------------------------------------------------------------------------
       select (select STRINGS.v from STRINGS where STRINGS.ID = LST.GROUP_BY_NAME) as GroupBy,
       PST.PK, LST.*, PST.PARENTPK  from PST, LST where PARENTPK = 0 and LST.LK = PST.LK

       --- Simple duplicate entries in PST ----------------------------------------------------------------------------
       select count(*) from (select PK, count(pk) as Num FROM PST GROUP BY PK HAVING (COUNT(pk)>1) ORDER BY Num)

       --- Different definitions of path keys -------------------------------------------------------------------------
       select count(*) from (
       select PK, count(pk) as Num from (
       select PK, LK, count(pk) as Num FROM PST GROUP BY PK,LK HAVING (COUNT(pk)>1)
       ) GROUP BY PK HAVING (COUNT(pk)>1)
       )

       select PK, count(pk) as Num from (
       select PK, LK, PARENTPK, count(pk) as Num FROM PST GROUP BY PK,LK,PARENTPK HAVING (COUNT(pk)>1)
       ) GROUP BY PK HAVING (COUNT(pk)>1)

       --- Duplicate entries in LST -----------------------------------------------------------------------------------
       select count(*) from (select LK, count(lk) as Num FROM LST GROUP BY LK HAVING (COUNT(lk)>1) ORDER BY Num)

       --- Children without defined parents ---------------------------------------------------------------------------

       create index pst_pk_index on PST (PK asc)

       Select children & their parents
       SELECT parent.PK, parent.PARENTPK, child.PK, child.PARENTPK from PST parent INNER JOIN PST child on parent.PK = child.PARENTPK

       Include those without parents:
       SELECT parent.PK, parent.PARENTPK, child.PK, child.PARENTPK from PST parent LEFT OUTER JOIN PST child on parent.PK = child.PARENTPK

       Show only nodes without children:
       SELECT parent.PK, parent.PARENTPK, child.PK, child.PARENTPK from PST parent LEFT OUTER JOIN PST child on parent.PK = child.PARENTPK WHERE child.PK IS NULL

       Without parents (can use FULL OUTER JOIN too):
       SELECT parent.PK, parent.PARENTPK, child.PK, child.PARENTPK from PST parent RIGHT OUTER JOIN PST child on parent.PK = child.PARENTPK WHERE parent.PK IS NULL


       PK's in summaries that aren't defined:
       //select a_day.PK,PST.PK from a_day inner join PST on a_day.PK = PST.PK -- only shows connected rows
       select distinct a_day.PK as summaryPK,PST.PK as PstPK from a_day left outer join PST on a_day.PK = PST.PK

       --- Misc ---
       Pagination of summaries:
       select * from (SELECT * FROM Summaries  WHERE Duration=2678400000 AND STARTTIME=to_date('2005-12-31 16:00', 'YYYY-MM-DD HH24:MI') ORDER BY Duration DESC, StartTime ASC) WHERE rownum <= 1000

       select distinct(duration) from summaries:
       86400000
       604800000
       2678400000
       3600000

       --- Generate parentPK -> PK mapping from summaries -------------------------------------------------------------

       insert into genpst select distinct child.PK as childPK, parent.PK as parentPK, null, child.nodedataid, child.recordid, child.trendedfields, child.treetypes, child.childrentypes from a_day parent right outer join a_day child on child.POS between parent.firstchildpos and parent.lastchildpos

       root = 6433757895833157633           (same as 6433757895833157600?, lk=748987996)

       other root = 3413571115585372200 (lk=1471133804)

       UPTO HERE:

       Why are hosts directly below GB?
       The PST is missing SOOOO many entries.  Need to not drop them when Diag server starts up

       */

      /*
       Here are some SQL I was using to clear up & understand the Boeing archive:

       Remove duplicates from PST

       CREATE TABLE PST2 (PK NUMBER, ParentPK NUMBER, LK INTEGER);
       INSERT INTO PST2 SELECT distinct * FROM PST;
       truncate table PST;
       INSERT INTO PST SELECT * FROM PST2;
       drop table PST2;
       purge recyclebin;

       Set up for analysis:
       create index pst_pk_index on PST (PK asc)
       create index summary_duration_index on SUMMARIES (Duration desc)

       Remove duplicates from summaries table:


       2005 yearly in view 'year05'
       create view year05 as SELECT * FROM nowSummaries  WHERE Duration=2678400000 AND STARTTIME=to_date('2005-12-31 16:00', 'YYYY-MM-DD HH24:MI') ORDER BY Duration DESC, StartTime ASC
       create view a_day as select * from nowsummaries where duration=2678400000 and starttime=to_date('2005-12-31 16:00', 'YYYY-MM-DD HH24:MI') ORDER BY Duration DESC, StartTime ASC

       insert into SummaryFiles select distinct Duration,StartTime from summaries

       CREATE TABLE GENPST (PK NUMBER, ParentPK NUMBER, LK INTEGER, nodedataid integer, recordid integer, trendedfields number, treetypes number, childrentypes integer);

       alter table genpst2 rename to genpst



       */


    }
    catch (Exception x) {
      e.close();
    }
  }



  private boolean getAutoRotate(String[] commandArgs) throws IOException {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-autorotate")) {
        deletionBatchFile = commandArgs[i + 1];
        deletionBatchOptions = new PrintStream(new FileOutputStream(deletionBatchFile));
        return true;
      }
    }
    return false;
  }



  private boolean getAutoFix(String[] commandArgs) throws IOException {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-autofix")) {
        return true;
      }
    }
    return false;
  }



  private void getDbOptions(String[] commandArgs) {
    this.dbTablePrefix = null;
    this.dbAltTablePrefix = null;
    this.backupSymbolDir = null;
    this.dbOracleURL = null;
    this.dbOracleUser = null;
    this.dbOraclePassword = null;

    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-dbOracleUrl")) {
        i++;
        this.dbOracleURL = commandArgs[i];
      }
      else if (commandArgs[i].equals("-dbOracleUser")) {
        i++;
        this.dbOracleUser = commandArgs[i];
      }
      else if (commandArgs[i].equals("-dbOraclePwd")) {
        i++;
        this.dbOraclePassword = commandArgs[i];
      }
      else if (commandArgs[i].equals("-dbPrefix")) {
        i++;
        this.dbTablePrefix = commandArgs[i];
      }
      else if (commandArgs[i].equals("-dbAltPrefix")) {
        i++;
        this.dbAltTablePrefix = commandArgs[i];
      }
      else if (commandArgs[i].equals("-backupSymbols")) {
        i++;
        //  Used to provide an alternate place to look for local symbol definitions.
        //  Not all symbol information is stored in the database currently.
        this.backupSymbolDir = commandArgs[i];
      }
    }
  }



  private void getEntityConditions(String[] commandArgs) {
    entityConditions.clear();
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-ec")) {
        EntityCondition cond = new EntityCondition();

        if (commandArgs[i + 1].equals("dataclass")) {
          cond.entity = EntityCondition.NodeDataClass;
        }
        else if (commandArgs[i + 1].equals("pathkey")) {
          cond.entity = EntityCondition.PathKey;
        }
        else if (commandArgs[i + 1].equals("localkey")) {
          cond.entity = EntityCondition.LocalKey;
        }
        else {
          throw new IllegalArgumentException("Unknown entity type for entity condition");
        }

        if (commandArgs[i + 2].equals("eq")) {
          cond.condition = Condition.EQUALS;
        }
        else if (commandArgs[i + 2].equals("ne")) {
          cond.condition = Condition.NOT_EQUALS;
        }
        else {
          throw new IllegalArgumentException("Unknown comparison type for entity condition");
        }

        cond.value = commandArgs[i + 3];
        entityConditions.add(cond);
        i += 3;
      }
    }
  }



  private void getFieldConditions(String[] commandArgs) {
    summaryConditions.clear();
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-fc")) {
        FieldCondition cond = new FieldCondition();

        getConditionField(commandArgs, i, cond);

        getConditionCondition(commandArgs, i, cond);

        cond.value = Double.parseDouble(commandArgs[i + 3]);
        summaryConditions.add(cond);
        i += 3;
      }
    }
  }



  private void getStatOutput(String[] commandArgs) {
    summaryStats = false;
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-stats")) {
        summaryStats = true;
      }
    }
  }



  private void getFieldModifications(String[] commandArgs) {
    rewriteConditions.clear();
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-mc")) {
        FieldModificationCondition cond = new FieldModificationCondition();

        getConditionField(commandArgs, i, cond);

        getConditionCondition(commandArgs, i, cond);

        cond.value = Double.parseDouble(commandArgs[i + 3]);
        cond.newvalue = Double.parseDouble(commandArgs[i + 4]);
        rewriteConditions.add(cond);
        i += 4;
      }
    }
  }



  private void getConditionCondition(String[] commandArgs, int i, FieldCondition cond) {
    if (commandArgs[i + 2].equals("eq")) {
      cond.condition = Condition.EQUALS;
    }
    else if (commandArgs[i + 2].equals("gt")) {
      cond.condition = Condition.GREATER_THAN;
    }
    else if (commandArgs[i + 2].equals("lt")) {
      cond.condition = Condition.LESS_THAN;
    }
    else if (commandArgs[i + 2].equals("ne")) {
      cond.condition = Condition.NOT_EQUALS;
    }
    else {
      throw new IllegalArgumentException("Unknown comparison type for field condition");
    }
  }



  private void getConditionField(String[] commandArgs, int i, FieldCondition cond) {
    if (commandArgs[i + 1].equals("value")) {
      cond.field = TrendedFields.VALUE_FIELD;
    }
    else if (commandArgs[i + 1].equals("count")) {
      cond.field = TrendedFields.COUNT_FIELD;
    }
    else if (commandArgs[i + 1].equals("min")) {
      cond.field = TrendedFields.MIN_FIELD;
    }
    else if (commandArgs[i + 1].equals("max")) {
      cond.field = TrendedFields.MAX_FIELD;
    }
    else if (commandArgs[i + 1].equals("sumsq")) {
      cond.field = TrendedFields.SUMOFSQUARES_FIELD;
    }
    else if (commandArgs[i + 1].equals("exc")) {
      cond.field = TrendedFields.EXCEPTIONS_FIELD;
    }
    else if (commandArgs[i + 1].equals("tout")) {
      cond.field = TrendedFields.TIMEOUTS_FIELD;
    }
    else {
      throw new IllegalArgumentException("Unknown field type for field condition");
    }
  }



  private String getSpecificFileOptions(String[] commandArgs) {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-f")) {
        if (commandArgs[i + 1].startsWith("\"")) {
          String arg = commandArgs[++i].substring(1);

          while (!commandArgs[i].endsWith("\"")) {
            arg = arg + " " + commandArgs[++i];
          }
          return arg.substring(0, arg.length() - 1);
        }
        return commandArgs[++i];
      }
    }
    return null;
  }



  private String getCustomerOptions(String[] commandArgs) {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-cn")) {
        return commandArgs[++i];
      }
    }
    return null;
  }

  private long getDeleteEntityOptions(String[] commandArgs) {
    return getPkFromCommandArgs(commandArgs);
  }

  private long getPkFromCommandArgs(String[] commandArgs) {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-pk")) {
        return Long.parseLong(commandArgs[++i]);
      }
    }
    return 0;
  }

  private HashSet<MajorMinor> getDurationOptions(String[] commandArgs) {
    MajorDuration majors[] = persistence.storageManager.getMajorDurations();
    HashSet<MajorMinor> durations = new HashSet<MajorMinor>();
    boolean specified = false;

    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-d")) {
        specified = true;
        String durs = commandArgs[++i];
        String[] dursplit = durs.split(",");

        MajorDuration major = majors[Integer.decode(dursplit[0])];

        if (dursplit.length > 1) {
          int minor = Integer.decode(dursplit[1]);
          MajorMinor mm = new MajorMinor(major, major.getMinorDurations()[minor]);

          durations.add(mm);
        }
        else {
          for (IGranularityData minor : major.getMinorDurations()) {
            durations.add(new MajorMinor(major, minor));
          }
        }
      }
    }

    if (!specified) {
      for (MajorDuration major : majors) {
        for (IGranularityData minor : major.getMinorDurations()) {
          durations.add(new MajorMinor(major, minor));
        }
      }
    }
    return durations;
  }



  private void dumpSummaries(PrintStream outputOptions, String customerName,
      HashSet<MajorMinor> durationOptions, String specificFileOptions) throws Exception {

    GroupBy[] groupBys = persistence.getStorageManager().getGroupBys();

    if (dumpHeaders) {
      outputOptions.print("FileName,type,name,PathKey,LocalKey,PositionInFile,FirstChildPosition,LastChildPosition,RecordId");

      for (int i = 0; i < TrendedFields.FIELD_COUNT; i++) {
        String field = TrendedFields.Helper.getFieldName(1 << i);

        outputOptions.print(",");
        outputOptions.print(field);
        outputOptions.print(", trend position (");
        outputOptions.print(field);
        outputOptions.print(")");
      }
      outputOptions.println();
    }

    PooledExecutor executor = getSummaryInsertionExecutor(dbTablePrefix + "Summaries");

    for (GroupBy groupBy : groupBys) {

      if (customerName == null || (groupBy.getName().startsWith(customerName))) {

        for (MajorMinor mm : durationOptions) {
          File[] summaryFiles = persistence.getStorageManager().getFileList(
              groupBy, mm.major, mm.minor, IStorageManager.Suffix.SUMMARY_SUFFIXES);

          for (int j = 0; j < summaryFiles.length; j++) {
            try {
              dumpSummaryFile(summaryFiles[j], outputOptions, mm.major, executor);
            }
            catch (Throwable e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
    if (null != executor) {
      executor.shutdownAfterProcessingCurrentlyQueuedTasks();
    }
  }



  public PooledExecutor getSummaryInsertionExecutor(final String prefixedTableName) throws Exception {

    if (dbTablePrefix == null) {
      return null;
    }
    //  Make sure the table is there
    Exporter e = null;

    try {
      if (this.dbOracleURL == null) {
        e = new Exporter(dbTablePrefix);
      }
      else {
        e = new Exporter(this.dbOracleURL, this.dbOracleUser, this.dbOraclePassword, this.dbTablePrefix);
      }
      e.createSummaryTable(prefixedTableName);
    }
    catch (Exception ex) {
      throw ex;
    }
    finally {
      if (e != null) {
        e.close();
      }
    }

    PooledExecutor executor = createDbInsertionExector(new ThreadFactory() {
                                                         public Thread newThread(Runnable r) {
                                                           try {
                                                             return new DbInsertionThread(r, dbOracleURL, dbOracleUser, dbOraclePassword, dbTablePrefix) {
                                                               protected PreparedStatement createInsertStatement() throws SQLException {
                                                                 return e.c.prepareStatement("INSERT INTO " + prefixedTableName + " (StartTime, Duration, PK, NodeDataId," +
                                                                     "RecordId, Pos, FirstChildPos, LastChildPos, TrendedFields, TreeTypes," +
                                                                     "ChildrenTypes, LastModified, " +
                                                                     "Total, TotalTrendPos, " +
                                                                     "Count, CountTrendPos, " +
                                                                     "Min, MinTrendPos, " +
                                                                     "Max, MaxTrendPos, " +
                                                                     "SumOfSquares, SumOfSquaresTrendPos, " +
                                                                     "Exceptions, ExceptionsTrendPos, " +
                                                                     "Timeouts, TimeoutsTrendPos, " +
                                                                     "TotalCpu, TotalCpuTrendPos, " +
                                                                     "CpuCount, CpuCountTrendPos, LastTrendOffset ) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )");
                                                               }



                                                               @Override protected PreparedStatement createSecondInsertStatement() throws SQLException {
                                                                 return e.c.prepareStatement("INSERT INTO " + prefixedTableName + Exporter.SUMMARY_TREE_TABLE_SUFFIX + " (StartTime, Duration, PK, Type, TypeId, InvocationTime, Value, Offset ) VALUES (?,?,?,?,?,?,?,?)");
                                                               }
                                                             };
                                                           }
                                                           catch (Exception e1) {
                                                             throw new RuntimeException(e1);
                                                           }
                                                         }
                                                       }
    );

    return executor;
  }



  private void dumpSummaryFile(File summaryFile, PrintStream outputOptions,
      MajorDuration major, Executor executor) throws IOException, InterruptedException {
    DurationSummaryIterator durIter = persistence.getStorageManager().getDurationSummaryIterator(summaryFile, IStorageManager.LockMode.SHARED);

    File writableFile = null;

    long fileFieldCount = 0;
    boolean extrapolateTo5m = false;

    if (summaryFile.getName().contains("1d")) {
      extrapolateTo5m = true;
    }

    HashMap<Class, Integer> dataTypeCounts = new HashMap<Class, Integer>();
    HashMap<Class, Integer> dataTypes = new HashMap<Class, Integer>();

    FileStorageMedium med = null;

    if (summaryStats) {
      try {
        med = persistence.getStorageManager().getTreeStorageMediumFromSummaryFile(summaryFile);
      }
      catch (Exception e) {
        System.err.println("No tree file for " + summaryFile.getName());
      }
    }
    if (med != null) {
      System.out.println(med);
    }

    outputOptions.flush();
    System.out.print("Dumping " + summaryFile.toString() + "...   ");
    System.out.flush();

    final long fileStartTime = IStorageManager.Helper.getDurationStartTimeFromFileName(summaryFile.getName());
    final long trendResolution = major.getTrendResolution(persistence.getStorageManager().getMajorDurations(), fileStartTime);
    final int trendPoints = major.getTrendPoints(persistence.getStorageManager().getMajorDurations(), fileStartTime);
    final long summaryDuration = major.getDuration(fileStartTime);

    if (false) {
      TrendFile trendFile = null;

      //if (summaryStats) {
      try {
        trendFile = persistence.getStorageManager().getTrendFromSummaryFile(summaryFile, major, fileStartTime);
      }
      catch (Exception e) {
        System.err.println("No trend file for " + summaryFile.getName());
      }
      //}
    }

    WritableDurationSummaryIterator writableDurIter = null;

    if (rewriteSummaries) {
      if (IStorageManager.Suffix.COMPRESSED.contains(summaryFile.getName())) {
        writableFile = new File(summaryFile.getPath().substring(0, summaryFile.getPath().lastIndexOf(".zip")) + ".dumper");
      }
      else {
        writableFile = new File(summaryFile.getPath() + ".dumper");
      }
      outputOptions.println("Rewriting summary file " + summaryFile + " to " + writableFile);
      writableDurIter =
          persistence.getStorageManager().createWritableDurationSummaryIterator(
              writableFile, durIter.getAggregatedBuckets(),
              IStorageManager.LockMode.EXCLUSIVE);
    }
    long eps = 0;

    int exportCount = 0;
    int count = 0;

    while (durIter.hasNext()) {
      NodeHolder holder = durIter.next();

      count++;

      if (null != holder) {

        if (rewriteSummaries) {
          if (autoFix) {
            fixAutomatically(holder);
          }
          else if (pathedFix) {
            holder = fixedPathed(holder, outputOptions, writableFile);
          }
          else {
            rewriteHolderValue(holder);
          }
        }
        else if (shouldSkip(holder)) {
          continue;
        }

        if (rewriteSummaries) {
          writableDurIter.append(holder, false);
        }
        else {
          final long nodeKey = holder.getPathedKey();
          final int localId = ApHash.getLocalKey(persistence.getSymbolManager().getPathedValue(nodeKey));
          INodeData data = null;
          Integer thisTypeValue = 0;
          Integer thisTypeDataValue = 0;

          try {
            if (localId != 0) {
              data = persistence.getSymbolManager().getValue(localId);
              thisTypeValue = dataTypeCounts.get(data.getClass());
              thisTypeDataValue = dataTypes.get(data.getClass());
            }
          }
          catch (Exception e) {
            e.printStackTrace();
            System.err.println("For key " + localId);
          }
          String value;

          if (data == null) {
            value = "<<unkown>>";
          }
          else {
            value = data.getQueryPathKey() + ",\"" + data.getName() + "\"";
          }

          if (!summaryStats) {
            outputOptions.print(summaryFile.getPath().substring(persistence.getStorageManager().getPersistencyDir().getPath().length()));
            outputOptions.print("," + value);

            outputOptions.print("," + nodeKey + "," + localId + "," + holder.getWrittenAtPosition() + "," + holder.getFirstChildPosition() + "," + holder.getLastChildPosition() + "," + holder.getRecordId());

            if (executor != null) {
              writeNodeHolderToDb(executor, fileStartTime, summaryDuration, holder, durIter.getLastTrendOffsetPosition());
              exportCount++;
            }
          }

          int thisTypeCount = 0;
          int thisTypeNonFieldCount = 0;

          if (thisTypeValue != null) {
            thisTypeCount = thisTypeValue.intValue();
          }
          if (thisTypeDataValue != null) {
            thisTypeNonFieldCount = thisTypeDataValue.intValue();
            thisTypeNonFieldCount++;
          }

          //  If you're trying to do trend analysis, you can uncomment the below & change the
          //  if( false ) above
          //double pointsInTrendWithData = getNumberOfPointsWithData(trendFile, holder, trendPoints);

          int localCount = 0;

          for (short i = 0; i < TrendedFields.FIELD_COUNT; i++) {
            if (holder.hasField(1 << i)) {
              localCount++;
              fileFieldCount++;
              if (thisTypeValue != null) {
                thisTypeCount++;
              }

              if (!summaryStats) {
                outputOptions.print(",");
                outputOptions.print(holder.getFieldValue(1 << i));

                outputOptions.print(",");
                outputOptions.print(holder.getTrendPosition(1 << i));

                // lhfile.println(holder.getTrendPosition(1 << i));

              }
            }
            else {
              if (!summaryStats) {
                outputOptions.print(",,");
              }
            }
          }
          if (!summaryStats) {
            outputOptions.println();
          }
          else {
            if (data != null) {
              dataTypeCounts.put(data.getClass(), thisTypeCount);
              dataTypes.put(data.getClass(), thisTypeNonFieldCount);
            }
          }
        }
      }
    }
    if (writableDurIter != null) {
      writableDurIter.updateTopLevelLastNodePosition(durIter.getTopLevelLastNodePosition());
      writableDurIter.updateLastTrendOffsetPosition(durIter.getLastTrendOffsetPosition());
      writableDurIter.close();
    }
    durIter.close();

    System.out.println("   wrote " + exportCount + " of " + count + " entries to DB");

    if (summaryStats) {
      double epsDouble = (double)eps / (double)summaryDuration * 1000;

      //      if (extrapolateTo5m) {
      //        eps += fileFieldCount * 720 / 3600;
      //      }
      outputOptions.print(summaryFile.getName() + "," + epsDouble + "," + fileFieldCount);
      for (Class dataClass : dataTypeCounts.keySet()) {
        String className = dataClass.getName();

        className = className.substring(className.lastIndexOf('.') + 1, className.length());
        outputOptions.print("," + className + "," + dataTypes.get(dataClass) + "," + dataTypeCounts.get(dataClass));
      }
    }

    long methodsInTree = 0;
    long storageIndex = FileStorageMedium.HEADER_SIZE;

    if (med != null) {
      while (true) {
        try {
          InputBuffer in = new InputBuffer();

          med.bufferData(storageIndex, in);
          int size = in.readInt();

          storageIndex += size;
          byte version = in.readByte();
          //don't include the size of the block or the legacy compression field in the data
          byte[] data = new byte[size - 5];

          in.readFully(data);

          //if we have a tree to deserialize...do so
          if (null != data) {
            final LongCounterMetric methodCount = new LongCounterMetric("", "", "");

            in.bufferData(data, 0, data.length, ByteOrder.nativeOrder());

            Object ret = WritableReader.read(in, persistence.getSymbolManager(), null);

            InstanceTreeNode tree = (InstanceTreeNode)ret;

            new TreeTraversal(tree, new ITreeTraverser() {
              public Level enterNode(ITreeNode node, Level parent) {
                methodCount.increment();
                return new Level(node, parent);
              }



              public boolean exitNode(Level element) {
                return false;
                //nothing to do
              }
            }
            );

            methodsInTree += methodCount.getValue().longValue();
          }

        }
        catch (IOException e) {
          break;
        }
        catch (ClassCastException ee) {
          continue;
        }
        catch (BufferUnderflowException e) {
          break;
        }
      }
      //outputOptions.println("Methods in tree " + methodsInTree);
      long treeMeth = methodsInTree / (trendResolution / 1000);

      if (extrapolateTo5m) {
        treeMeth += methodsInTree / 5;
      }
      outputOptions.println("," + treeMeth);

      med.close();
    }
    else {
      //outputOptions.println();
    }

    if (rewriteSummaries && autoRotate) {
      File originalFile = new File(summaryFile.getPath());

      originalFile.delete();
      if (originalFile.exists()) {
        if (deletionBatchFile != null && deletionBatchOptions != null) {
          String deleteCommand = "";

          if (deletionBatchFile.endsWith(".cmd")) {
            deleteCommand = "delete ";
          }
          else {
            deleteCommand = "rm ";
          }
          deletionBatchOptions.println(deleteCommand + originalFile.getPath());
        }
      }
      if (IStorageManager.Suffix.SUMMARY_COMPRESSED.ends(summaryFile.getName())) {
        summaryFile = new File(IStorageManager.Suffix.SUMMARY_COMPRESSED.getBaseName(summaryFile.getPath()) + ".summary");
      }
      IStorageManager.Helper.rotate(writableFile, summaryFile);
    }
  }



  private NodeHolder fixedPathed(NodeHolder holder, PrintStream outputOptions, File writableFile) {

    long pk = holder.getPathedKey();

    if (oldToNewMapping.containsKey(pk)) {
      long newPk = oldToNewMapping.get(pk);

      outputOptions.println("Rewriting pk " + pk + " to " + newPk + " in summary file " + writableFile);
      return new NodeHolder(newPk, holder);
    }

    return holder;
  }



  void writeNodeHolderToDb(Executor executor, final long fileStartTime, final long summaryDuration, final NodeHolder holder, final int lastTrendOffsetPosition) throws InterruptedException {
    executor.execute(new Runnable() {
                       public void run() {
                         try {
                           DbInsertionThread thread = (DbInsertionThread)Thread.currentThread();

                           thread.pstInsert.clearParameters();

                           final Timestamp sqlTimestamp = new Timestamp(fileStartTime);

                           thread.pstInsert.setTimestamp(1, sqlTimestamp); // startTime
                           thread.pstInsert.setLong(2, summaryDuration); // duration
                           thread.pstInsert.setLong(3, holder.getPathedKey()); // PK
                           thread.pstInsert.setInt(4, holder.getNodeDataId()); // Type of the node data (eg WriteableReader.ID_MetricData)
                           thread.pstInsert.setInt(5, holder.getRecordId()); // Type of the record (eg WritableReader.ID_MetricTrendRecord)
                           thread.pstInsert.setLong(6, holder.getWrittenAtPosition()); // position in summary file
                           thread.pstInsert.setLong(7, holder.getFirstChildPosition()); // position of firstChild
                           thread.pstInsert.setLong(8, holder.getLastChildPosition()); // position of lastChild
                           thread.pstInsert.setInt(9, holder.getTrendedFields()); // Bitmask for the fields this node trends
                           thread.pstInsert.setInt(10, holder.getTreeTypes()); // Bitmask of the available trees for this node
                           thread.pstInsert.setLong(11, holder.getChildrenTypes()); // Bitmask for the child types
                           thread.pstInsert.setTimestamp(12, new Timestamp(holder.getLastModified()));

                           if (PersistedRecord.FIELD_COUNT != 9) {
                             throw new IllegalStateException("Exporter out of date with schema");
                           }

                           for (int i = 0; i < PersistedRecord.FIELD_COUNT; i++) {
                             int base_col = getBaseColumnForField(i);
                             double value = holder.getFieldValue(1 << i);
                             long trendPos = holder.getTrendPosition(1 << i);

                             if (Double.isNaN(value)) {
                               //  DB can't handle NaN, just use 0 for now
                               value = 0;
                             }

                             thread.pstInsert.setDouble(base_col, value);
                             thread.pstInsert.setLong(base_col + 1, trendPos);
                           }

                           thread.pstInsert.setInt(31, lastTrendOffsetPosition);

                           try {
                             thread.pstInsert.executeUpdate();
                           }
                           catch (SQLException e) {
                             e.printStackTrace();
                           }

                           //          ArrayList<TreeRecord> al = new ArrayList<TreeRecord>();

                           for (int treeNum = 0; treeNum < holder.getNumOfTrees(); treeNum++) {
                             TreeData treeData = holder.getTreeDataForInstancePosition(treeNum);

                             if (treeData == null) {
                               continue;
                             }

                             thread.insert2.clearParameters();
                             thread.insert2.setTimestamp(1, sqlTimestamp); // startTime
                             thread.insert2.setLong(2, summaryDuration); // duration
                             thread.insert2.setLong(3, holder.getPathedKey()); // PK

                             thread.insert2.setInt(4, InstanceTreeType.getTreeMaskForTreeType(treeData.getType()));
                             thread.insert2.setByte(5, treeData.getTypeId()); // index, distinguishes between multiple max trees when we're keeping both
                             thread.insert2.setLong(6, holder.getTreeStartTime(treeNum)); // in microseconds
                             thread.insert2.setDouble(7, holder.getTreeValue(treeNum));
                             thread.insert2.setLong(8, holder.getTreeOffset(treeNum));

                             thread.insert2.executeUpdate();
                           }
                         }
                         catch (Exception e) {
                           e.printStackTrace();
                         }
                       }
                     }
    );
  }



  public static int getBaseColumnForField(int i) {
    int base_col = 13 + i * 2;

    return base_col;
  }



  /** Count the number of trend points for the holder that have an actual data */
  private double getNumberOfPointsWithData(TrendFile trendFile, NodeHolder holder, int trendPoints) {
    double pointsInTrendWithData = Double.NaN;

    if (trendFile != null) {
      if (holder.hasField(TrendedFields.COUNT_FIELD)) {
        pointsInTrendWithData = 0;
        long trendPos = holder.getTrendPosition(TrendedFields.COUNT_FIELD);
        double[] sparsityArr = trendFile.read(trendPos, 0, trendPoints, trendPoints);

        for (double val : sparsityArr) {
          if (!Double.isNaN(val) && val > 0) {
            pointsInTrendWithData++;
          }
        }
      }
    }
    return pointsInTrendWithData;
  }



  private void fixAutomatically(NodeHolder holder) {
    double value = holder.getFieldValue(TrendedFields.VALUE_FIELD);
    double min = holder.getFieldValue(TrendedFields.MIN_FIELD);
    double count = holder.getFieldValue(TrendedFields.COUNT_FIELD);
    double max = holder.getFieldValue(TrendedFields.MAX_FIELD);

    long nodeKey = holder.getPathedKey();
    int localId = ApHash.getLocalKey(persistence.getSymbolManager().getPathedValue(nodeKey));
    INodeData data = persistence.getSymbolManager().getValue(localId);

    if (!(data instanceof MetricData)) {
      if (value < 0) {
        holder.setFieldValue(Double.NaN, TrendedFields.VALUE_FIELD);
        holder.setFieldValue(Double.NaN, TrendedFields.COUNT_FIELD);
        value = Double.NaN;
      }
      if (min < 0 || (min == Integer.MAX_VALUE && Double.isNaN(value))) {
        holder.setFieldValue(Double.NaN, TrendedFields.MIN_FIELD);
        min = Double.NaN;
      }
      double avg = Double.NaN;

      if (count > 0 && (value / count) > 0) {
        avg = value / count;
      }

      if ((max < min && !Double.isNaN(min)) || (max < avg && !Double.isNaN(avg)) || (max < 0)) {
        holder.setFieldValue(Double.NaN, TrendedFields.MAX_FIELD);
        if (!Double.isNaN(avg)) {
          holder.setFieldValue(avg, TrendedFields.MAX_FIELD);
        }
      }
    }

    /*
     double newMax = max;
     double newMin = min;
     double newVal = value;
     if (min < 0) {
     newMin = rotateNegativeInt(min); //LH - I deleted this method because it is fundamentally broken
     }

     if (max < 0 && max != Integer.MIN_VALUE) {
     newMax = rotateNegativeInt(max);
     }

     if (value < 0) {
     newVal = rotateNegativeInt(value);
     }

     max = Math.max(newMax,newMin);
     max = Math.max(max,newVal);

     min = Math.min(max,newMin);
     if (min < 0) {
     System.err.println("Min is still negative " + max + "," + newMin);
     }
     min = Math.min(min,newVal);

     value = newVal;

     holder.setFieldValue(value,TrendedFields.VALUE_FIELD);
     holder.setFieldValue(min,TrendedFields.MIN_FIELD);
     holder.setFieldValue(max,TrendedFields.MAX_FIELD);

     */
  }



  private void rewriteHolderValue(NodeHolder holder) {
    for (FieldModificationCondition modificationCondition : rewriteConditions) {
      double fieldVal = holder.getFieldValue(modificationCondition.field);

      switch (modificationCondition.condition) {
        case FieldCondition.EQUALS:
          if (fieldVal == modificationCondition.value) {
            if (!shouldSkip(holder)) {
              holder.setFieldValue(modificationCondition.newvalue, modificationCondition.field);
            }
          }
          break;

        case FieldCondition.GREATER_THAN:
          if (fieldVal > modificationCondition.value) {
            if (!shouldSkip(holder)) {
              holder.setFieldValue(modificationCondition.newvalue, modificationCondition.field);
            }
          }
          break;

        case FieldCondition.LESS_THAN:
          if (fieldVal < modificationCondition.value) {
            if (!shouldSkip(holder)) {
              holder.setFieldValue(modificationCondition.newvalue, modificationCondition.field);
            }
          }
          break;

        case FieldCondition.NOT_EQUALS:
          if (fieldVal != modificationCondition.value) {
            if (!shouldSkip(holder)) {
              holder.setFieldValue(modificationCondition.newvalue, modificationCondition.field);
            }
          }
          break;

      }
    }
  }



  private boolean shouldSkip(NodeHolder holder) {
    boolean skip;

    if (summaryConditions.size() > 0 || entityConditions.size() > 0) {
      skip = true;
    }
    else {
      return false;
    }

    Iterator<FieldCondition> iter = summaryConditions.iterator();

    while (skip && iter.hasNext()) {
      FieldCondition fc = iter.next();
      double fieldVal = holder.getFieldValue(fc.field);

      switch (fc.condition) {
        case Condition.EQUALS:
          if (fieldVal == fc.value) {
            skip = false;
          }
          break;

        case Condition.GREATER_THAN:
          if (fieldVal > fc.value) {
            skip = false;
          }
          break;

        case Condition.LESS_THAN:
          if (fieldVal < fc.value) {
            skip = false;
          }
          break;

        case Condition.NOT_EQUALS:
          if (fieldVal != fc.value) {
            skip = false;
          }
          break;
      }
    }

    if (skip && summaryConditions.size() > 0) {
      return skip;
    }

    if (entityConditions.size() > 0) {
      skip = true;
    }

    long nodeKey = holder.getPathedKey();
    int localId = ApHash.getLocalKey(persistence.getSymbolManager().getPathedValue(nodeKey));
    INodeData data = persistence.getSymbolManager().getValue(localId);

    for (EntityCondition fc : entityConditions) {
      String holderValue = null;

      switch (fc.entity) {
        case EntityCondition.NodeDataClass:
          holderValue = data.getClass().getSimpleName();
          break;

        case EntityCondition.PathKey:
          holderValue = String.valueOf(nodeKey);
          break;

        case EntityCondition.LocalKey:
          holderValue = String.valueOf(localId);
          break;
      }

      switch (fc.condition) {
        case Condition.EQUALS:
          if (holderValue.equals(fc.value)) {
            skip = false;
          }
          break;

        case Condition.NOT_EQUALS:
          if (!holderValue.equals(fc.value)) {
            skip = false;
          }
          break;
      }
    }
    return skip;
  }



  private void dumpChildren(String[] commandArgs, PrintStream outputOptions) {

    Long pk = null;

    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-pk")) {
        pk = new Long(Long.parseLong(commandArgs[i + 1]));
      }
    }

    if (pk == null) {
      outputOptions.println("Must specify -pk <pathkey>");
    }
    else {
      int[] children = ((SymbolManager)persistence.getSymbolManager()).getChildLocalKeys(pk);

      for (int i = 0; i < children.length; i++) {
        outputOptions.println("LK=" + children[i] + ":\t" + persistence.getSymbolManager().getValue(children[i]).getDisplayName());
      }
    }
  }



  private void dumpPathedSymbolTable(PrintStream outputOptions) throws Exception {
    if (dumpHeaders) {
      outputOptions.println("PathKey,ParentKey,LocalKey,NodeData");
    }

    PooledExecutor executor = createDbInsertionExector(new ThreadFactory() {
                                                         public Thread newThread(Runnable r) {
                                                           try {
                                                             return new DbInsertionThread(r, dbOracleURL, dbOracleUser, dbOraclePassword, dbTablePrefix) {
                                                               protected PreparedStatement createInsertStatement() throws SQLException {
                                                                 return e.c.prepareStatement("INSERT INTO " + dbTablePrefix + "PST ( PK, ParentPK, LK) VALUES ( ?, ?, ? )");
                                                               }
                                                             };
                                                           }
                                                           catch (Exception e1) {
                                                             throw new RuntimeException(e1);
                                                           }
                                                         }
                                                       }
    );

    IntLongIntHashIterator iterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();
    int i = 0;

    while (iterator.hasNext()) {
      final IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();

      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      final long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      final int localKey = ApHash.getLocalKey(value);

      final long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);

      outputOptions.print(realpk + "," + parentKey + "," + localKey + ",");

      INodeData data = null;
      try {
        data = persistence.getSymbolManager().getValue(localKey);
      }
      catch (Throwable t) {
        t.printStackTrace(outputOptions);
        continue;
      }

      outputOptions.println(data.getClass().getName()+":"+data.describe());

      if (executor != null) {
        executor.execute(new Runnable() {
                           public void run() {

                             try {
                               DbInsertionThread thread = (DbInsertionThread)Thread.currentThread();

                               thread.pstInsert.clearParameters();
                               thread.pstInsert.setDouble(1, realpk);
                               thread.pstInsert.setDouble(2, parentKey);
                               thread.pstInsert.setInt(3, localKey);
                               thread.pstInsert.executeUpdate();
                             }
                             catch (Exception e) {
                               e.printStackTrace();
                             }
                           }
                         }
        );
      }
    }

    if (executor != null) {
      executor.shutdownAfterProcessingCurrentlyQueuedTasks();
      executor.awaitTerminationAfterShutdown();
    }
  }



  private PooledExecutor createDbInsertionExector(ThreadFactory threadFactory) {
    if (dbTablePrefix == null) {
      return null;
    }

    return Exporter.createDbInsertionExecutor(threadFactory);
  }



  private void dumpPathedSymbolTableTree(PrintStream outputOptions) {
    IntLongIntHashIterator iterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    TreeMap<Long, ArrayList<Pair<Long, Integer>>> parentChildMap = new TreeMap<Long, ArrayList<Pair<Long, Integer>>>();

    while (iterator.hasNext()) {
      IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      int localKey = ApHash.getLocalKey(value);

      long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);

      ArrayList<Pair<Long, Integer>> al = parentChildMap.get(parentKey);

      if (al == null) {
        al = new ArrayList();
        parentChildMap.put(parentKey, al);
      }
      al.add(new Pair(realpk, localKey));
    }

    ArrayList<Long> theChildren = new ArrayList<Long>(parentChildMap.keySet());

    {
      outputOptions.print("root ( pk=0 ) {");
      ArrayList<Pair<Long, Integer>> children = parentChildMap.get(0L);

      parentChildMap.remove(0L);
      for (Pair<Long, Integer> child : children) {
        dumpPathedEntry(outputOptions, parentChildMap, 0, child.getLeft(), child.getRight());
      }
      outputOptions.println();
      outputOptions.println("}");
    }

    for (long parentPK : theChildren) {
      ArrayList<Pair<Long, Integer>> children = parentChildMap.get(parentPK);

      if (children == null) {
        //  this node was already removed from the treemap, don't
        continue;
      }

      parentChildMap.remove(parentPK);

      outputOptions.print("disconnected ( pk=" + parentPK + " ) {");

      for (Pair<Long, Integer> child : children) {
        dumpPathedEntry(outputOptions, parentChildMap, 0, child.getLeft(), child.getRight());
      }

      outputOptions.println();
      outputOptions.println("}");
    }
  }



  private void spaces(PrintStream ps, int spaces) {
    for (int i = 0; i < spaces; i++) {
      ps.print(INDENT_CHAR);
    }
  }



  private String describe(INodeData nd, long pk) {
    if (nd instanceof FragmentData || nd instanceof ProbeGroupData || nd instanceof ProbeData || nd instanceof GroupBy || nd instanceof LayerData || nd instanceof HostData) {
      return nd.getQueryPathKey() + " " + nd.getDisplayName();
    }
    else if (MetricData.isStatusMetric(nd)) {
      StringBuffer desc = new StringBuffer(nd.getQueryPathKey() + " " + nd.getDisplayName());
      String threshold = nd.getInfo("" + pk, NIIKey.threshold.toString());

      desc.append(null != threshold ? " [threshold = " + threshold + "]" : "");
      return desc.toString();
    }
    else if (nd instanceof MetricData) {
      return nd.getQueryPathKey() + " " + nd.getDisplayName();
    }
    else if (nd instanceof SqlStatementData || nd instanceof ProbeArcData || nd instanceof GenericFragmentArcData) {
      return nd.getQueryPathKey();
    }
    else {
      return nd.describe();
    }
  }



  private boolean dumpPathedEntry(PrintStream outputOptions, TreeMap<Long, ArrayList<Pair<Long, Integer>>> parentChildMap, int indent, long pk, int lk) {

    ArrayList<Pair<Long, Integer>> children = parentChildMap.get(pk);

    parentChildMap.remove(pk);

    outputOptions.println();

    spaces(outputOptions, indent);
    outputOptions.print("node( pk=" + pk + ", lk=" + lk + " ");

    try {
      INodeData nd = persistence.getSymbolManager().getValue(lk);

      outputOptions.print(" " + describe(nd, pk));
    }
    catch (Exception e) {
      outputOptions.print(" " + e.toString());
    }

    outputOptions.print(" ) {");

    boolean didoutput = false;

    if (null != children) {
      for (Pair<Long, Integer> child : children) {
        didoutput |= dumpPathedEntry(outputOptions, parentChildMap, indent + 1, child.getLeft(), child.getRight());
      }
    }

    if (didoutput) {
      outputOptions.println();
      spaces(outputOptions, indent);
    }

    outputOptions.print("}");
    return true;
  }



  /**
   * Outputs a kind of statistical distribution of the contents of this TSDB archive.  I
   * use this for measuring the average number of fragments per probe, or the number
   * of probes per probe group, etc.
   *
   * @param outputOptions
   */
  private void dumpPathedSymbolTableStats(PrintStream outputOptions) {

    IntLongIntHashIterator iterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    TreeMap<String, MutableLong> counts = new TreeMap<String, MutableLong>();

    while (iterator.hasNext()) {
      IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      int localKey = ApHash.getLocalKey(value);

      long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);

      List<INodeData> path;

      try {
        path = persistence.getSymbolManager().getNodeDataPathFromPathKey(realpk);
      }
      catch (NoSuchElementException nsee) {
        outputOptions.println("No def for PK# " + realpk + ", looking up LK " + localKey + "...");

        try {
          INodeData nd = persistence.getSymbolManager().getValue(localKey);

          outputOptions.println("\tOK: " + nd.describe());
        }
        catch (Exception e) {
          outputOptions.println("\tfailed: " + e.toString());
        }

        continue;
      }

      StringBuffer pathsb = new StringBuffer();

      for (INodeData nd : path) {
        pathsb.append('/');
        final String org;

        if (nd instanceof GroupBy) {
          org = nd.getName();
        }
        else if (nd instanceof ProbeData) {
          org = nd.describe();
        }
        else if (nd instanceof FragmentData) {
          MethodData md = ((FragmentData)nd).getIdentifyingMethodData();

          if (md != null) {
            String args = md.getArgument();
            LayerData ld = md.getLayerData();

            if (ld != null) {
              if (args == null) {
                org = "fragment_" + ld.getName().replace('/', ':');
              }
              else {
                org = "fragment_" + ld.getName().replace('/', ':') + "+arg";
              }
            }
            else {
              if (args == null) {
                org = "fragment_no_layer";
              }
              else {
                org = "fragment+arg";
              }
            }
          }
          else {
            org = "fragment_no_root";
          }
        }
        else {
          org = nd.getQueryPathKey();
        }

        pathsb.append(org);
      }

      String paths = pathsb.toString();
      MutableLong ml = counts.get(paths);

      if (ml == null) {
        ml = new MutableLong(1);
        counts.put(paths, ml);
      }
      else {
        ml.increment(1);
      }
    }

    if (dumpHeaders) {
      outputOptions.println("Path,Occurances");
    }

    for (Map.Entry<String, MutableLong> me : counts.entrySet()) {
      outputOptions.println(me.getKey() + "," + me.getValue());
    }
  }



  private void dumpSymbolOfType(PrintStream stream) throws Exception {
    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream("C:\\Users\\golaniz\\Desktop\\Misc\\QCIM1I108840_LLoyds\\dumpSymbolOfType.utxt"));
    try {
      if (dumpHeaders) {
        stream.println("count,LocalKey,Direction,Target,Describe");
        bos.write("count,LocalKey,Direction,Target,Describe\n".getBytes());
      }

      int max_items_to_print = Integer.MAX_VALUE;
      int counter = 0;
      int errcount = 0;

      SymbolManager symbolManager = (SymbolManager) persistence.getSymbolManager();
      IntLongIntHashIterator iterator = symbolManager.getPathedSymbolTableIterator();

      while (iterator.hasNext() && counter<max_items_to_print ) {
        final IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
        long value = (Long)entry.getValue();
        final long parentKey = symbolManager.getParentComponentOfPathValue(value);
        final int localKey = ApHash.getLocalKey(value);
        final long realpk = symbolManager.getPathKey(parentKey, localKey);

        try {
          INodeData nodeData = symbolManager.getNodeDataFromPathKey(realpk);

          //if (nodeData instanceof FragmentArcData) {
          //  counter++;
          //  printFragmentArcData(bos, counter, symbolManager, realpk, (FragmentArcData)nodeData);
          //  printCounter(stream, counter);
          //}

          if (nodeData instanceof OutboundProbeArcData) {
            counter++;
            printOutboundProbeArcData(bos, counter, symbolManager, realpk, (OutboundProbeArcData) nodeData);
            printCounter(stream, counter);
          }


        } catch (Exception ignored) {
          bos.write((++errcount + DELIMITER + localKey + DELIMITER + realpk + DELIMITER + "1972-02-02" + DELIMITER + "err\n").getBytes());
          stream.print(counter + ",");
        }

      }
    } finally {
      bos.flush();
      bos.close();
    }
  }

  private void printOutboundProbeArcData(BufferedOutputStream bos, int counter, SymbolManager symbolManager, long realpk, OutboundProbeArcData nodeData) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(counter);
    buf.append(DELIMITER);
    buf.append(nodeData.getLocalKey());
    buf.append(DELIMITER);
    buf.append(realpk);
    buf.append(DELIMITER);
    buf.append(SDF.format(nodeData.getLastModified(realpk)));
    buf.append(DELIMITER);

    String probeGroup="N/A", probeName="N/A";
    long[] ancestors = symbolManager.getParentPathFromPathKey(realpk);

    for (long pk : ancestors) {
      if (pk!=0) {
        INodeData ancestorNodeData = symbolManager.getNodeDataFromPathKey(pk);
        //buf.append(ancestorNodeData.getClass().getSimpleName()).append("[").append(ancestorNodeData.getNodeIdentifier().toString()).append("]/~/");
        if (ancestorNodeData instanceof ProbeGroupData) {
          probeGroup = ancestorNodeData.getName();
        }
        else if (ancestorNodeData instanceof OutboundProbeArcData) {
          probeName = ancestorNodeData.getNodeIdentifier().getValueAtIndex(1); // the constructor of OutboundProbeArcData puts the [source.getName] in the second item of the [identifier] array
        }
      }
    }
    buf.append(probeGroup);
    buf.append(DELIMITER);
    buf.append(probeName);
    buf.append(DELIMITER);
    buf.append(symbolManager.getStringPathFromPathKey(realpk));
    buf.append(DELIMITER);
    buf.append(nodeData.getDirectionString());
    buf.append(DELIMITER);
    buf.append(nodeData.getTargetDisplayName());
    buf.append(DELIMITER);
    buf.append(nodeData.getTargetName());
    buf.append(DELIMITER);
    buf.append(nodeData.getTargetType());
    buf.append(DELIMITER);
    buf.append(nodeData.getDistributedSource());
    buf.append(DELIMITER);
    buf.append(nodeData.describe());
    buf.append("\n");
    bos.write(buf.toString().getBytes());
  }

  private void printCounter(PrintStream stream, int count) {
    if (count%100==0) {
      stream.println(count + ",");
    }
    else {
      stream.print(count + ",");
    }
  }

  private void printFragmentArcData(BufferedOutputStream bos, int counter, SymbolManager symbolManager, long realpk, FragmentArcData nodeData) throws IOException {
    StringBuilder buf = new StringBuilder();
    buf.append(counter);
    buf.append(DELIMITER);
    buf.append(nodeData.getLocalKey());
    buf.append(DELIMITER);
    buf.append(realpk);
    buf.append(DELIMITER);
    buf.append(SDF.format(nodeData.getLastModified(realpk)));
    buf.append(DELIMITER);

    String probeGroup="N/A", probeName="N/A";
    long[] ancestors = symbolManager.getParentPathFromPathKey(realpk);

    for (long pk : ancestors) {
      if (pk!=0) {
        INodeData ancestorNodeData = symbolManager.getNodeDataFromPathKey(pk);
        //buf.append(ancestorNodeData.getClass().getSimpleName()).append("[").append(ancestorNodeData.getNodeIdentifier().toString()).append("]/~/");
        if (ancestorNodeData instanceof ProbeGroupData) {
          probeGroup = ancestorNodeData.getName();
        }
        else if (ancestorNodeData instanceof OutboundProbeArcData) {
          probeName = ancestorNodeData.getNodeIdentifier().getValueAtIndex(1); // the constructor of OutboundProbeArcData puts the [source.getName] in the second item of the [identifier] array
        }
      }
    }
    buf.append(probeGroup);
    buf.append(DELIMITER);
    buf.append(probeName);
    buf.append(DELIMITER);
    buf.append(symbolManager.getStringPathFromPathKey(realpk));
    buf.append(DELIMITER);
    buf.append(nodeData.getDirectionString());
    buf.append(DELIMITER);
    buf.append(nodeData.getTargetDisplayName());
    buf.append(DELIMITER);
    buf.append(nodeData.describe());
    buf.append("\n");
    bos.write(buf.toString().getBytes());
  }

  private void dumpAllSymbols(PrintStream outputOptions) {
    if (dumpHeaders) {
      outputOptions.println("Index, LocalKey, LastModified, ClassName, DisplayName, Name, Describe");
    }

    int count = 0;

    ClosingIterator<INodeData> iterator = ((SymbolManager)persistence.getSymbolManager()).getSymbolTableIterator();

    try {
      while (iterator.hasNext() ) {
        INodeData data = iterator.next();
        count++;

        String lastModified = data.getInfo(NIIKey.last_modified.toString(),
            NodeDataFactory.UNSPECIFIED_PATH_KEY_STRING);
        Date time = null;

        if (null != lastModified) {
          time = new Date(Long.parseLong(lastModified));
        }

        int symbolToken = data.getNodeIdentifier().getSymbolToken();

        outputOptions.printf(
            "%d#%d#%s#%s#%s#%s#%s%n",
            count,
            symbolToken,
            null != time ? SDF.format(time) : "",
            data.getClass().getName(),
            data.getDisplayName(),
            data.getName(),
            data.describe());



      }
    }
    finally {
      outputOptions.println("Symbol table contains " + count + " local symbols");
      iterator.close();
    }
  }

  private void dumpSymbolTable(PrintStream outputOptions) throws Exception {
    if (dumpHeaders) {
      outputOptions.println("LocalKey,Last Modified,NodeData: name,values");
    }

    int count = 0;

    HashMap<String, MutableInteger> stringMap = new HashMap<String, MutableInteger>();

    ClosingIterator<INodeData> iterator = ((SymbolManager)persistence.getSymbolManager()).getSymbolTableIterator();

    PreparedStatement lstInsert = null;
    PreparedStatement stringQuery = null;
    PreparedStatement stringInsert = null;
    Exporter e = null;

    if (dbTablePrefix != null) {
      e = new Exporter(this.dbOracleURL, this.dbOracleUser, this.dbOraclePassword, this.dbTablePrefix);

      e.createLST(dbTablePrefix + "LST");

      StringBuffer cols = new StringBuffer("LK, identifier");
      StringBuffer vals = new StringBuffer("?, ?");
      int num = 1;

      for (Field f : Exporter.getIdentifiers()) {
        cols.append(", ");
        vals.append(", ");

        cols.append(f.getName());
        vals.append("?");
        num++;
      }

      lstInsert = e.c.prepareStatement("INSERT INTO " + dbTablePrefix + "LST ( " + cols + " ) VALUES ( " + vals + ")");
      stringQuery = e.c.prepareStatement("SELECT ID FROM strings WHERE hash = ? AND v = ?");
      stringInsert = e.c.prepareStatement("INSERT INTO strings ( ID, hash, v ) VALUES ( strings_sequence.nextVal, ?, ? )");
      //      System.out.println( "found " + num + " LST fields" );
    }

    try {
      while (iterator.hasNext()) {
        INodeData data = iterator.next();

        //  Lookup in string table
        NodeIdentifier ni = data.getNodeIdentifier();

        if (e != null) {
          lstInsert.clearParameters();
          lstInsert.setInt(1, ni.getSymbolToken());
          lstInsert.setLong(2, ni.getIdentifier());

          for (int i = 0; i < NodeIdentifier.MAX_BIT_SET; i++) {
            String s = ni.getValue(1L << i);
            int string_id = 0;

            if (s != null && s.length() > 0) {
              //  Look up the string

              if (s.length() > 4000) {
                //  Can't insert strings > 4k into Oracle without a CLOB...
                s = s.substring(0, 3997) + "...";
              }

              MutableInteger stringId = stringMap.get(s);

              if (stringId != null) {
                string_id = stringId.intValue();
              }
              else {
                stringQuery.setInt(1, s.hashCode());
                stringQuery.setString(2, s);

                ResultSet rs;

                try {
                  rs = stringQuery.executeQuery();
                }
                catch (SQLException sqle) {
                  sqle.printStackTrace();
                  throw sqle;
                }

                if (!rs.next()) {
                  rs.close();

                  stringInsert.clearParameters();
                  stringInsert.setInt(1, s.hashCode());
                  stringInsert.setString(2, s);
                  stringInsert.executeUpdate();

                  //                rs = stringInsert.getGeneratedKeys();
                  rs = stringQuery.executeQuery();

                  if (!rs.next()) {
                    throw new IllegalStateException("Error inserting into strings: " + s);
                  }
                }
                else {
                  int a = 1 + 1;
                }

                string_id = rs.getInt(1);
                stringMap.put(s, new MutableInteger(string_id));
                rs.close();
              }
            }
            lstInsert.setInt(i + 3, string_id);
          }

          try {
            lstInsert.executeUpdate();
          }
          catch (java.sql.SQLException sqle) {
            outputOptions.println(sqle.toString());
          }
        }

        count++;

        String lastModified = data.getInfo(NIIKey.last_modified.toString(),
            NodeDataFactory.UNSPECIFIED_PATH_KEY_STRING);
        Date time = null;

        if (null != lastModified) {
          time = new Date(Long.parseLong(lastModified));
        }

        int symbolToken = data.getNodeIdentifier().getSymbolToken();

        outputOptions.println(symbolToken +
            (0 > symbolToken ? ":" + data.hashCode() : "") +
            "," + (null != time ? time : "") + "," + data.getClass().getName() + ":" +
            data.getDisplayName() + ": " + data.describe());

        if (symbolToken < 0) {
          INodeData colliding = persistence.getSymbolManager().getValue(data.hashCode());

          lastModified = colliding.getInfo(NIIKey.last_modified.toString(),
              NodeDataFactory.UNSPECIFIED_PATH_KEY_STRING);
          time = null;
          if (null != lastModified) {
            time = new Date(Long.parseLong(lastModified));
          }
          symbolToken = colliding.getNodeIdentifier().getSymbolToken();
          outputOptions.println("\tcollides with: " + symbolToken + "," +
              (null != time ? time : "") + "," + colliding.getClass().getName() + ":" +
              colliding.getDisplayName() + ": " + colliding.describe());
        }
      }
    }
    finally {
      outputOptions.println("Symbol table contains " + count + " local symbols");
      iterator.close();

      if (lstInsert != null) {
        lstInsert.close();
      }
      if (stringQuery != null) {
        stringQuery.close();
      }
      if (stringInsert != null) {
        stringInsert.close();
      }

      if (e != null) {
        e.close();
      }
    }
  }

  private void dumpSingleSymbol(PrintStream outputOptions, long pk) throws Exception {
    SymbolManager symbolManager = (SymbolManager) persistence.getSymbolManager();
    INodeData nodeData = symbolManager.getNodeDataFromPathKey(pk);
    outputOptions.println(nodeData.describe());
    Set<String> keys = nodeData.getAllKeys(pk);
    for (String key : keys) {
      outputOptions.println("\t"+key+"=["+nodeData.getValueForKey(key)+"]");
    }
  }


  /**
   * Dump out a breakdown of local symbols by node data, to help give some indicators of where symbol explosions might be happening.
   * @param outputOptions
   * @throws Exception
   */
  private void countSymbolTableBreakdown(PrintStream outputOptions) throws Exception {

    final Map<String, Integer> nodeDataTypeCountPerClass = new HashMap<String, Integer>();

    int counter = 0;

    ClosingIterator<INodeData> iterator = ((SymbolManager) persistence
        .getSymbolManager()).getSymbolTableIterator();

    try {
      while (iterator.hasNext()) {
        INodeData nodeData = iterator.next();

        // Lookup in string table
        NodeIdentifier ni = nodeData.getNodeIdentifier();

        counter++;

        Integer count = nodeDataTypeCountPerClass.get(nodeData.getClass()
            .getName());

        if (count == null) {
          nodeDataTypeCountPerClass.put(nodeData.getClass().getName(), 1);
        } else {
          count++;
          nodeDataTypeCountPerClass.put(nodeData.getClass().getName(), count);
        }


      }

      outputOptions.println("# nodes :\t\t  Node Data Type");

      ArrayList<Entry<String, Integer>> as = new ArrayList<Entry<String, Integer>>(
          nodeDataTypeCountPerClass.entrySet());

      // sort by largest types
      Collections.sort(as, new Comparator() {
        public int compare(Object o1, Object o2) {
          Map.Entry e1 = (Map.Entry) o1;
          Map.Entry e2 = (Map.Entry) o2;
          Integer first = (Integer) e1.getValue();
          Integer second = (Integer) e2.getValue();
          return first.compareTo(second);
        }
      });

      for (Map.Entry<String, Integer> entry : as) {
        outputOptions.println( entry.getValue() + " :\t\t " + entry.getKey());
      }
    } finally {
      outputOptions.println("Symbol table contains " + counter
          + " local symbols");
      iterator.close();
    }
  }



  private boolean shouldDumpHeaders(String[] commandArgs) {
    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-noheaders")) {
        return false;
      }
    }
    return true;
  }



  private PrintStream getOutputOptions(String[] commandArgs) throws FileNotFoundException {
    PrintStream ps;

    for (int i = 0; i < commandArgs.length; i++) {
      if (commandArgs[i].equals("-o")) {
        String toFile = commandArgs[i + 1];

        if (toFile.equalsIgnoreCase("db")) {
          try {
            Exporter e = new Exporter("");

            e.close();
          }
          catch (Exception e1) {
            e1.printStackTrace();
            throw new IllegalArgumentException(e1.toString());
          }

          ps = new PrintStream(new NullOutputStream());
        }
        else {
          ps = new PrintStream(new FileOutputStream(toFile), false);
        }
        outputToStdOut = false;
        return ps;
      }
    }
    outputToStdOut = true;
    return new PrintStream(System.out);
  }



  private PersistenceDumper(PropertiesUtil props) throws Exception {
    persistence = Persistence.createDumperInstance(props);
    server = new DiagnosticsServer(properties, new MonitorModule(new Monitor()));
  }



  PersistenceDumper(DiagnosticsServer server, Persistence p) {
    if (server == null || p == null) {
      throw new NullPointerException();
    }
    this.server = server;
    persistence = p;
  }



  private void rewritePathkeys(PrintStream outputOptions, String customerName, HashSet<MajorMinor> durationOptions, String fname) {

    try {
      // create the source Symbol manager, will only contain the patehed
      // symbol table, (not the local symbol table)
      SymbolManager sourceSymbolManger = new SymbolManager(fname, null, 1);
      SymbolManager destinationSymbolManger = ((SymbolManager)persistence.getSymbolManager());

      IntLongIntHashIterator iterator = sourceSymbolManger.getPathedSymbolTableIterator();

      // iterate over all the source records and
      while (iterator.hasNext()) {
        IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
        int pathKey = (Integer)entry.getKey(); // path w/o generation
        long pathedValue = (Long)entry.getValue(); // pathed value () parent&local w/o generation

        long sourceParentKey = sourceSymbolManger.getParentKey(pathedValue); // w generation
        int sourceLocalKey = ApHash.getLocalKey(pathedValue);
        long sourceRealpk = sourceSymbolManger.getPathKey(sourceParentKey, sourceLocalKey);

        int sourcePathKeyGenerationId = sourceSymbolManger.getGenerationId(pathKey);
        int destinationPathKeyGenerationId = destinationSymbolManger.getGenerationId(pathKey);

        // pathkey already in destination symbol table
        if (destinationSymbolManger.containsPathKey(sourceRealpk)) {
          continue;
        }

        // make sure pathkey dosen't exist in destaination with different generation id
        if (destinationPathKeyGenerationId != 0
            && destinationPathKeyGenerationId != sourcePathKeyGenerationId) {
          long destinationParentKey = sourceSymbolManger.getParentKey(pathedValue); // w generation
          int destinationLocalKey = ApHash.getLocalKey(pathedValue);
          long destinationRealpk = destinationSymbolManger.getPathKey(destinationParentKey, destinationLocalKey);

          this.oldToNewMapping.put(sourceRealpk, destinationRealpk);
        }
      }

      dumpSummaries(outputOptions, customerName, durationOptions, "");

    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }



  /**
   * Merge the give pathed symbol file into the current pathed file
   *
   * @param outputOptions - where to print the results
   * @param fname full path filename to the source pathed symbol table file
   */
  private void mergePathedSymbolTable(PrintStream outputOptions, String fname) {

    try {
      // create the source Symbol manager, will only contain the patehed
      // symbol table, (not the local symbol table)
      SymbolManager sourceSymbolManger = new SymbolManager(fname, null, 1);
      SymbolManager destinationSymbolManger = ((SymbolManager)persistence.getSymbolManager());

      IntLongIntHashIterator iterator = sourceSymbolManger.getPathedSymbolTableIterator();

      // iterate over all the source records and
      // add them to the destination symbol manager's
      // pathed table
      while (iterator.hasNext()) {
        IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
        int pathKey = (Integer)entry.getKey(); // path w/o generation
        long pathedValue = (Long)entry.getValue(); // pathed value () parent&local w/o generation

        long sourceParentKey = sourceSymbolManger.getParentKey(pathedValue); // w generation
        int sourceLocalKey = ApHash.getLocalKey(pathedValue);
        long sourceRealpk = sourceSymbolManger.getPathKey(sourceParentKey, sourceLocalKey);

        int sourcePathKeyGenerationId = sourceSymbolManger.getGenerationId(pathKey);
        int destinationPathKeyGenerationId = destinationSymbolManger.getGenerationId(pathKey);

        // pathkey already in destination symbol table
        if (destinationSymbolManger.containsPathKey(sourceRealpk)) {
          continue;
        }

        // make sure pathkey dosen't exist in destaination with different generation id
        if (destinationPathKeyGenerationId != 0 && destinationPathKeyGenerationId != sourcePathKeyGenerationId) {
          System.err.println(" ERROR - PathKey found in destination symbol table with different genId " + sourceRealpk + "," + sourceParentKey + "," + sourceLocalKey);
          continue;
        }

        String displayName = getLocalKeyDisplayName(destinationSymbolManger, sourceLocalKey);

        if (displayName == null) {
          System.err.println(" ERROR - Local key not found in destination symbol table " + sourceRealpk + "," + sourceParentKey + "," + sourceLocalKey);
          continue;
        }

        try {
          // don't forget to add the metadata
          long metaData = sourceSymbolManger.getKeyMetaData(sourceRealpk);

          // add the record
          destinationSymbolManger.updateSymbol(sourceRealpk, pathKey, pathedValue, metaData);

          // success, print it.
          outputOptions.println("Added record to destination symbol table " + "pk:" + sourceRealpk + "," + "parentKey:" + sourceParentKey + "," + "lk:" + sourceLocalKey
              + "," + "name" + displayName + "," + "md:" + metaData);

        }
        catch (IOException e) {
          e.printStackTrace();
        }

      }
    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }



  private String getLocalKeyDisplayName(SymbolManager symbolManager, int localKey) {
    // if the local symbol key is not found getValue will throw a
    // RuntimeException
    try {
      return symbolManager.getValue(localKey).getDisplayName();
    }
    catch (RuntimeException e) {
      return null;
    }
  }



  private void deleteOrphanedNodes(PrintStream outputOptions) {
    IntLongIntHashIterator iterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    TreeMap<Long, ArrayList<Pair<Long, Integer>>> parentChildMap = new TreeMap<Long, ArrayList<Pair<Long, Integer>>>();
    SymbolManager symbolManger = ((SymbolManager)persistence.getSymbolManager());

    while (iterator.hasNext()) {
      IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)iterator.next();
      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      long parentKey = symbolManger.getParentComponentOfPathValue(value);
      int localKey = ApHash.getLocalKey(value);

      long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);

      ArrayList<Pair<Long, Integer>> al = parentChildMap.get(parentKey);

      if (al == null) {
        al = new ArrayList();
        parentChildMap.put(parentKey, al);
      }
      al.add(new Pair(realpk, localKey));
    }

    // walk the tree from groupby
    // and remove all children
    ArrayList<Pair<Long, Integer>> children = parentChildMap.get(0L);

    parentChildMap.remove(0L);
    for (Pair<Long, Integer> child : children) {
      removeChildrenFromTree(outputOptions, symbolManger, parentChildMap, child.getLeft(), child.getRight(), false);
    }

    // everything left must be 
    // orphaned, so delete it
    ArrayList<Long> theChildren = new ArrayList<Long>(parentChildMap.keySet());

    for (long parentPK : theChildren) {

      children = parentChildMap.get(parentPK);

      if (children == null) {
        //  this node was already removed from the treemap, don't
        continue;
      }

      parentChildMap.remove(parentPK);

      for (Pair<Long, Integer> child : children) {
        removeChildrenFromTree(outputOptions, symbolManger, parentChildMap, child.getLeft(), child.getRight(), true);
      }

    }
  }


  /*
   * Attempt to find local symbols that are not referenced in the path symbol table. 
   * TODO: - Deal w/ instance trees/MethodDatas (apparently they have local keys as well)
   *       - Local keys referenced as part of the arcs (this may be a problem when an entity is deleted via purging... 
   *         the code in SQE for deleting an entity takes care of finding the arcs when a probe is deleted
   */
  private void gcTest(PrintStream outputOptions) throws Exception {

    // Build a list of all symbols from the LST and match the symbol against all PST entries
    // Left over symbols from the LST are the ones that are not reachable anymore

    HashSet<Integer> symbolTokens = new HashSet<Integer>(); // all symbols in the LST
    HashSet<Integer> endpointSymbolTokens = new HashSet<Integer>(); // all endpoints contained in ArcDatas

    // Walk the LST
    ClosingIterator<INodeData> lstIterator = ((SymbolManager)persistence.getSymbolManager()).getSymbolTableIterator();
    while (lstIterator.hasNext()) {
      INodeData data = lstIterator.next();

      // Skipping datas that are inside TreeDatas
      if (data instanceof MethodData) continue;

      int symbolToken = data.getNodeIdentifier().getSymbolToken();

      symbolTokens.add(symbolToken);

      // record endpoints
      if (data instanceof AbstractNodeArcData) {
        Integer endpointSymbolToken = ((AbstractNodeArcData)data).getEndpoint().getNodeIdentifier().getSymbolToken();
        endpointSymbolTokens.add(endpointSymbolToken);
      }

    }
    lstIterator.close();

    int numberOfSymbols = symbolTokens.size();


    // Walk the PST
    IntLongIntHashIterator pstIterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    while (pstIterator.hasNext()) {
      final IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)pstIterator.next();

      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      final long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      final int localKey = ApHash.getLocalKey(value);

      // If the symbol is defined in the PST, take it out of the LST set
      int symbolTokenInPst = persistence.getSymbolManager().getValue(localKey).getNodeIdentifier().getSymbolToken();
      if (symbolTokens.contains(symbolTokenInPst)) {
        symbolTokens.remove(symbolTokenInPst);
      }
      // else might already be removed from the set

      // If the PST symbol is found in the endpoint map, take it out
      if (endpointSymbolTokens.contains(symbolTokenInPst)) {
        endpointSymbolTokens.remove(symbolTokenInPst);
      }
    }

    // Left over symbols may be garbage
    for (Integer symbolToken : symbolTokens) {
      INodeData nodeData = persistence.getSymbolManager().getValue(symbolToken);
      if (nodeData != null)
        outputOptions.println(symbolToken+","+nodeData.getClass().getName()+":"+nodeData.describe());
      else
        outputOptions.println(symbolToken);
    }

    outputOptions.println("Found "+symbolTokens.size()+" out of "+numberOfSymbols+" symbols unreachable");
    outputOptions.println();

    outputOptions.println("Found "+endpointSymbolTokens.size()+" arcs with unreachable endpoints:");
    for (Integer symbolToken : endpointSymbolTokens) {
      INodeData nodeData = persistence.getSymbolManager().getValue(symbolToken);
      if (nodeData != null)
        outputOptions.println(symbolToken+","+nodeData.getClass().getName()+":"+nodeData.describe());
      else
        outputOptions.println(symbolToken);
    }





  }


  private void countSRs(PrintStream outputOptions) throws Exception {

    Map<INodeData, Integer> srCountPerProbe = new HashMap<INodeData, Integer>();

    // Walk the PST
    IntLongIntHashIterator pstIterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    while (pstIterator.hasNext()) {
      final IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)pstIterator.next();

      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      final long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      final int localKey = ApHash.getLocalKey(value);

      INodeData nodeData = persistence.getSymbolManager().getValue(localKey);
      if (nodeData instanceof FragmentData) {
        final long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);
        try {
          List<INodeData> path = persistence.getSymbolManager().getNodeDataPathFromPathKey(realpk);

          if (path.size() > 2 && path.get(path.size()-2) instanceof ProbeData) {
            ProbeData probeData = (ProbeData)path.get(path.size()-2);
            Integer cnt = srCountPerProbe.get(probeData);
            if (cnt == null) cnt = 0;
            cnt++;

            srCountPerProbe.put(probeData, cnt);
          }
        }
        catch (NoSuchElementException e)
        {
          // Deleted?
        }
      }
    }


    outputOptions.println("Probe Name:# of Server Requests");
    for (Map.Entry<INodeData, Integer> entry : srCountPerProbe.entrySet()) {
      outputOptions.println(entry.getKey().getDisplayName()+": "+entry.getValue());

    }

  }

  /**
   * Give a breakdown by node type of entries in the PST.  
   * @param outputOptions
   * @throws Exception
   */

  private void countPSTSymbolsByType(PrintStream outputOptions) throws Exception {


    Map<String, Integer> nodeDataTypeCountPerClass =  new HashMap<String, Integer>();
    Map<INodeData, Map<String, Integer>> classTypeCountPerProbe =  new HashMap<INodeData, Map<String, Integer>>();



    int counter =0;
    // Walk the PST
    IntLongIntHashIterator pstIterator = ((SymbolManager)persistence.getSymbolManager()).getPathedSymbolTableIterator();

    while (pstIterator.hasNext()) {

      final IntLongIntHashIterator.IntLongEntry entry = (IntLongIntHashIterator.IntLongEntry)pstIterator.next();

      int pathkey = (Integer)entry.getKey();
      long value = (Long)entry.getValue();
      final long parentKey = persistence.getSymbolManager().getParentComponentOfPathValue(value);
      final int localKey = ApHash.getLocalKey(value);


      INodeData nodeData = persistence.getSymbolManager().getValue(localKey);


      Integer count = nodeDataTypeCountPerClass.get(nodeData.getClass().getName());

      if (count == null) {
        nodeDataTypeCountPerClass.put(nodeData.getClass().getName(), 1);
      } else {
        count++;
        nodeDataTypeCountPerClass.put(nodeData.getClass().getName(), count);
      }

      counter++;


      final long realpk = persistence.getSymbolManager().getPathKey(parentKey, localKey);

      List<INodeData> path = null;
      try {
        path = persistence.getSymbolManager().getNodeDataPathFromPathKey(realpk);
      } catch (NoSuchElementException e) {
        // symbol may have been purged from PST (deleted via UI or by purging), skip counting it
        continue;
      }


      for (int i = 0; i < path.size()-1; i++ ) {
        // Look for and group counts under any probe entity up the path
        if (path.get(i) instanceof ProbeData) {
          ProbeData probeData = (ProbeData)path.get(i);
          Map<String, Integer> countsByClass = classTypeCountPerProbe.get(probeData);
          if (countsByClass == null) {
            countsByClass = new HashMap<String, Integer>();
            classTypeCountPerProbe.put(probeData, countsByClass);
          }

          Integer classCount = countsByClass.get(nodeData.getClass().getName());

          if (classCount == null) {
            countsByClass.put(nodeData.getClass().getName(), 1);
          } else {
            classCount++;
            countsByClass.put(nodeData.getClass().getName(), classCount);
          }

        }
      }

    }

    outputOptions.println("# nodes :\t\t  Pathed Symbol entry Node Data Type");



    ArrayList<Entry<String, Integer>> as = new ArrayList< Entry<String, Integer> > ( nodeDataTypeCountPerClass.entrySet() );

    Collections.sort( as , new Comparator() {
      public int compare( Object o1 , Object o2 )
      {
        Map.Entry e1 = (Map.Entry)o1 ;
        Map.Entry e2 = (Map.Entry)o2 ;
        Integer first = (Integer)e1.getValue();
        Integer second = (Integer)e2.getValue();
        return first.compareTo( second );
      }
    });

    for (Map.Entry<String, Integer> entry : as)
    {
      outputOptions.println( entry.getValue() + " :\t\t " + entry.getKey());
    }


    outputOptions.println("--------------------------------------------");
    outputOptions.println("Node Data Type broken down per probe:");
    outputOptions.println("--------------------------------------------");

    ArrayList<Entry<INodeData, Map<String, Integer>>> probes = new ArrayList< Entry<INodeData, Map<String, Integer>> > ( classTypeCountPerProbe.entrySet() );

    Collections.sort( probes , new Comparator() {
      public int compare( Object o1 , Object o2 )
      {
        Map.Entry e1 = (Map.Entry)o1 ;
        Map.Entry e2 = (Map.Entry)o2 ;
        String first = ((INodeData)e1.getKey()).getDisplayName();
        String second = ((INodeData)e2.getKey()).getDisplayName();
        return first.compareTo( second );
      }
    });

    for (Map.Entry<INodeData, Map<String, Integer>> entry : probes) {
      outputOptions.println(entry.getKey().getDisplayName() + ":");
      for (Map.Entry<String, Integer> subentry : entry.getValue().entrySet()) {
        outputOptions.println("\t " + subentry.getValue() +":\t "+ subentry.getKey());
      }
    }

    outputOptions.println("\nPST contains  "  + counter + " entries");


  }





  private void removeChildrenFromTree(PrintStream outputOptions, SymbolManager symbolManger, TreeMap<Long, ArrayList<Pair<Long, Integer>>> parentChildMap, long pk, int lk, boolean delete) {

    ArrayList<Pair<Long, Integer>> children = parentChildMap.get(pk);

    parentChildMap.remove(pk);
    if (delete) {
      symbolManger.deletePathKey(pk, true);
    }

    if (null != children) {
      for (Pair<Long, Integer> child : children) {
        removeChildrenFromTree(outputOptions, symbolManger, parentChildMap, child.getLeft(), child.getRight(), delete);
      }
    }
  }


  public void deleteEntity(PrintStream outputOptions, long pk) throws Exception
  {
    if (pk != 0) {
      // pathAwareSymbolFile.deleteKey(pathKey)
      boolean success = persistence.getSymbolManager().deletePathKey(pk, false);
      outputOptions.println(success);
    }
    else {
      outputOptions.println("Usage error: deletepk requires a pk=<pathkey> argument.");
    }
  }


  /**
   * Basic checks on symbol files
   * @param outputOptions
   * @param commandArgs
   */
  void checkArchive(PrintStream outputOptions, String[] commandArgs)
  {
    boolean doMoveFiles = false;
    if (commandArgs.length > 1 && commandArgs[1].equals("rename")) {
      doMoveFiles = true;
    }

    GroupBy[] groupBys = persistence.getStorageManager().getGroupBys();
    for (GroupBy groupBy : groupBys) {

      MajorDuration majors[] = persistence.storageManager.getMajorDurations();
      for (MajorDuration major : majors) {
        for (GranularityData minor : major.getMinorDurations()) {
          outputOptions.println();
          outputOptions.println(groupBy+": "+major.getName() + ":" + minor);
          File[] summaryFiles = persistence.getStorageManager().getFileList(
              groupBy, major, minor, IStorageManager.Suffix.SUMMARY_SUFFIXES);

          for (File summaryFile : summaryFiles) {
            outputOptions.print(".");

            try {
              DurationSummaryIterator durIter = persistence.getStorageManager().getDurationSummaryIterator(summaryFile, IStorageManager.LockMode.SHARED);
              HashSet<Long> positions = new HashSet<Long>(4000);
              HashSet<Long> noChildrenHolders = new HashSet<Long>(4000);

              int trendCorruptions = 0;

              while (durIter.hasNext()) {
                NodeHolder holder = durIter.next();

                if (null != holder) {
                  trendCorruptions = checkTrendPositions(holder);
                  positions.add(holder.getWrittenAtPosition());
                  if (holder.getFirstChildPosition() == -1) {
                    noChildrenHolders.add(holder.getPathedKey());
                  }
                }
              }

              try {
                durIter.reset();
              }
              catch (Throwable t) {
                t.printStackTrace(outputOptions);
              }

              long cnt = 0;
              while (durIter.hasNext()) {
                NodeHolder holder = durIter.next();

                if (null != holder) {

                  // Check if we have seen these children before (from prev. run)
                  if (holder.getFirstChildPosition() != -1) {
                    if (!positions.contains(holder.getFirstChildPosition())) {
                      cnt++;
                    }
                    if (!positions.contains(holder.getLastChildPosition())) {
                      cnt++;
                    }
                  }
                  long pathedVal = persistence.getSymbolManager().getPathedValue(holder.getPathedKey());
                  long parent = persistence.getSymbolManager().getParentComponentOfPathValue(pathedVal);

                  // Do we have the parent?
                  if (noChildrenHolders.contains(parent)) {
                    cnt++;
                  }
                }
              }
              durIter.close();


              if (cnt != 0 || trendCorruptions != 0) {
                outputOptions.println();
                outputOptions.println("Corruptions "+cnt+"/"+trendCorruptions+" "+summaryFile);

                if (doMoveFiles) {
                  outputOptions.println("\trename: "+summaryFile.renameTo(new File(summaryFile.getAbsoluteFile()+"__")));
                  File treeFile = persistence.getStorageManager().getTreeFileFromSummaryFile(summaryFile);
                  if (treeFile.exists()) {
                    outputOptions.println("\t"+treeFile);
                    outputOptions.println("\trename: "+treeFile.renameTo(new File(treeFile.getAbsoluteFile()+"__")));
                  }
                  File trendFile = getTrendFromSummaryFile(summaryFile);
                  if (trendFile.exists()) {
                    outputOptions.println("\t"+trendFile);
                    outputOptions.println("\trename: "+trendFile.renameTo(new File(trendFile.getAbsoluteFile()+"__")));
                  }
                }

              }
              outputOptions.flush();


            } catch (IOException e) {
              e.printStackTrace(outputOptions);
            }



          }
        }
      }
    }
  }

  private File getTrendFromSummaryFile(File summaryFile) {
    String baseName = summaryFile.getName().split("\\.")[0];

    File trendFile;

    if (Suffix.SUMMARY_COMPRESSED.ends(summaryFile.getName())) {
      trendFile = new File(summaryFile.getParentFile().getPath(), Suffix.TREND_COMPRESSED.append(baseName));
    }
    else {
      trendFile = new File(summaryFile.getParentFile().getPath(), Suffix.TREND.append(baseName));
    }
    return trendFile;
  }


  private int checkTrendPositions(NodeHolder holder) {
    int cnt = 0;

    int fieldBit = 1;

    int fields = holder.getFields();

    if (fields > 0) {
      for (int i = 0; i < TrendedFields.FIELD_COUNT; i++) {
        int field = (fields & fieldBit);

        if (field != 0) {
          long trendPosition = holder.getTrendPosition(field);

          if (trendPosition < -1 || (trendPosition > -1 && trendPosition < TrendFile.HEADER_SIZE)) {
            cnt++;
          }
        }
        fieldBit = fieldBit << 1;
      }
    }

    return cnt;
  }





  void hotspots(PrintStream outputOptions, String[] commandArgs)
  {
    final Map<String, Long> methodCount = new TreeMap<String, Long>();

    GroupBy[] groupBys = persistence.getStorageManager().getGroupBys();
    MajorDuration majors[] = persistence.storageManager.getMajorDurations();

    for (GroupBy groupBy : groupBys) {

      for (MajorDuration major : majors) {
        for (GranularityData minor : major.getMinorDurations()) {
          outputOptions.println();
          outputOptions.println(groupBy+": "+major.getName() + ":" + minor);
          File[] summaryFiles = persistence.getStorageManager().getFileList(
              groupBy, major, minor, IStorageManager.Suffix.SUMMARY_SUFFIXES);

          for (File summaryFile : summaryFiles) {
            System.out.println("***" + summaryFile);
            DurationSummaryIterator durIter;
            try {
              durIter = persistence.getStorageManager().getDurationSummaryIterator(summaryFile, IStorageManager.LockMode.SHARED);
              while (durIter.hasNext()) {
                NodeHolder holder = durIter.next();

                if (holder.getNumOfTrees() > 0) {
                  final long nodeKey = holder.getPathedKey();
                  final long pathKey = persistence.getSymbolManager().getPathedValue(nodeKey);
                  long parent = persistence.getSymbolManager().getParentComponentOfPathValue(pathKey);
                  final int localId = ApHash.getLocalKey(pathKey);
                  INodeData data = persistence.getSymbolManager().getValue(localId);
                  System.out.println(data);
                  List<INodeData> path = persistence.getSymbolManager().getNodeDataPathFromPathKey(persistence.getSymbolManager().getPathKey(parent, localId));

                  final String probeName = path.get(path.size()-2).getName();

                  for (int i=0; i<holder.getNumOfTrees(); i++) {
                    if (holder.getTreeOffset(i) != -1) {
                      TreeData treeData = holder.getInstanceTreeData(i);
                      System.out.println("\t"+treeData);
                      // FileStorageMedium treeFile = persistence.getStorageManager().getTreeStorageMedium(groupBy, major, holder.getTreeStartTime(i), IStorageManager.AccessMode.READ, IStorageManager.LockMode.BLOCKING_SHARED);
                      FileStorageMedium med = persistence.getStorageManager().getTreeStorageMediumFromSummaryFile(summaryFile);
                      if (med == null) continue;

                      //System.out.println("\t\ttree="+med);
                      Block block = holder.getInstanceTreeBlock(i, med);
                      StoredTreeRecord record = new StoredTreeRecord(new AggregateTreeNode(treeData, null));
                      record.setInstance(block,
                          holder.getTreeStartTime(i),
                          holder.getTreeTag(i),
                          (long)holder.getTreeValue(i));
                      record.setNotMissingCallees(); //not missing since persisted trees are stored fully correlated
                      InstanceTreeNode tree = record.getTree(new InputBuffer(), persistence.getSymbolManager());
                      // tree.debugDumpTree();
                      new TreeTraversal(tree, new ITreeTraverser() {
                        public Level enterNode(ITreeNode node, Level parent) {
                          InstanceTreeNode itn = (InstanceTreeNode)node;
                          String key = probeName + ":"+ node.getData().getName();
                          System.out.println(key);
                          Long cnt = methodCount.get(key);
                          if (cnt == null) {
                            cnt = new Long(0);
                          }
                          if (itn.getCpuTime() > 0) {
                            cnt = cnt + itn.getCpuTime();
                          }
                          methodCount.put(key, cnt);

                          System.out.println(itn.getData()+": "+itn.getCpuTime());


                          return new Level(node, parent);
                        }

                        public boolean exitNode(Level element) {
                          return false;  //nothing to do
                        }
                      }
                      );



                    }
                  }
                }
              }
            } catch (IOException e1) {
              // TODO Auto-generated catch block
              e1.printStackTrace();
            }





            //						 try {
            //							File tf = persistence.getStorageManager().getTreeFileFromSummaryFile(summaryFile);
            //							if (! tf.exists()) continue;
            //							FileStorageMedium med = persistence.getStorageManager().getTreeStorageMediumFromSummaryFile(summaryFile);
            //							if (med == null) continue;
            //
            //							// processTreeFile(med, treeCount);
            //						} catch (IOException e) {
            //							// TODO Auto-generated catch block
            //							e.printStackTrace();
            //						}

          }
        }
      }
    }

    for (Map.Entry<String, Long> method : methodCount.entrySet()) {
      System.out.println(method.getKey()+": "+method.getValue());
    }


  }

  void processTreeFile(FileStorageMedium med, HashMap<String, Long> treeCount)
  {

    long storageIndex = FileStorageMedium.HEADER_SIZE;

    while (true) {
      try {
        InputBuffer in = new InputBuffer();

        med.bufferData(storageIndex, in);
        int size = in.readInt();

        storageIndex += size;
        byte version = in.readByte();
        //don't include the size of the block or the legacy compression field in the data
        byte[] data = new byte[size - 5];

        in.readFully(data);

        //if we have a tree to deserialize...do so
        if (null != data) {
          final LongCounterMetric methodCount = new LongCounterMetric("", "", "");

          in.bufferData(data, 0, data.length, ByteOrder.nativeOrder());

          Object ret = WritableReader.read(in, persistence.getSymbolManager(), null);

          InstanceTreeNode tree = (InstanceTreeNode)ret;

          // tree.debugDumpTree(true);
          System.out.println(tree.getNode().getData());
          System.out.println(tree.getStartTime()+"-"+tree.getEndTime()+":"+tree.getData().getLocalKey());



          //          new TreeTraversal(tree, new ITreeTraverser() {
          //            public Level enterNode(ITreeNode node, Level parent) {
          //              methodCount.increment();
          //              return new Level(node, parent);
          //            }
          //
          //
          //
          //            public boolean exitNode(Level element) {
          //              return false;  //nothing to do
          //            }
          //          }
          //          );

        }

        med.close();


      }
      catch (IOException e) {
        break;
      }
      catch (ClassCastException ee) {
        continue;
      }
      catch (BufferUnderflowException e) {
        break;
      }
    }


  }

  static class NullOutputStream extends OutputStream {

    @Override public void write(byte b[]) throws IOException {
    }



    @Override public void write(byte b[], int off, int len) throws IOException {
    }



    public void write(int b) throws IOException {
    }
  }




  class Condition {
    static final int LESS_THAN = 0;
    static final int GREATER_THAN = 1;
    static final int EQUALS = 2;
    static final int NOT_EQUALS = 3;

    int condition;
  }




  class FieldCondition extends Condition {
    int field;
    double value;
  }




  class EntityCondition extends Condition {
    static final int NodeDataClass = 0;
    static final int LocalKey = 1;
    static final int PathKey = 2;

    int entity;
    String value;
  }




  class FieldModificationCondition extends FieldCondition {
    double newvalue;
  }

}
