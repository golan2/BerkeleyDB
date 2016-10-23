/*
 * (C) Copyright 2014 Hewlett-Packard Development Company, L.P. 
 * --------------------------------------------------------------------------
 * This file contains proprietary trade secrets of Hewlett Packard Company.
 * No part of this file may be reproduced or transmitted in any form or by
 * any means, electronic or mechanical, including photocopying and
 * recording, for any purpose without the expressed written permission of
 * Hewlett Packard Company.
 */


package com.mercury.diagnostics.server.util.migration;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.mercury.diagnostics.common.bootstrap.InstallUtil;
import com.mercury.diagnostics.common.data.graph.EntityKey;
import com.mercury.diagnostics.common.data.graph.EntityKeyHelper;
import com.mercury.diagnostics.common.data.graph.ISymbolManager;
import com.mercury.diagnostics.common.data.graph.Metric;
import com.mercury.diagnostics.common.data.graph.meta.Alert;
import com.mercury.diagnostics.common.data.graph.meta.INodeMetaDataItem;
import com.mercury.diagnostics.common.data.graph.meta.NodeMetaData;
import com.mercury.diagnostics.common.data.graph.meta.NodeMetaDataSummary;
import com.mercury.diagnostics.common.data.graph.node.GroupBy;
import com.mercury.diagnostics.common.data.graph.node.MetricData;
import com.mercury.diagnostics.common.io.ByteBufferConfig;
import com.mercury.diagnostics.common.io.INodeData;
import com.mercury.diagnostics.common.logging.DefaultLogFactory;
import com.mercury.diagnostics.common.logging.Level;
import com.mercury.diagnostics.common.logging.LoggerManager;
import com.mercury.diagnostics.common.scripting.groovy.GroovyUtil;
import com.mercury.diagnostics.common.util.DiagProperties;
import com.mercury.diagnostics.common.util.NullLogger;
import com.mercury.diagnostics.server.metadata.impl.MetaDataPersistence.MetaDataFileReader;
import com.mercury.diagnostics.server.persistence.impl.Persistence;
import com.mercury.diagnostics.server.thresholding.ImportExportManager;

/**
 * Utility class to migrate pre-9.23 alert definitions (which had been persisted
 * with Retroweaver) to 9.23 and higher. The tool reads the existing alert definitions
 * from the metadata files and generates an import file that can be imported via the
 * standard thresholding/once mechanism
 * 
 * @author flachbar
 *
 */
public class AlertMigration {

  // for determining the install directory
  private final static String EXAMPLE_PATH = "lib/mediator.jar";

  // the server install directory
  private String serverDir;
  
  // reference to the symbol manager
  private ISymbolManager symbolManager;
  
  // the file to export to
  private String exportFile;
  private DiagProperties props;

  public static void main(String[] args) {

    if (args.length != 1) {
      System.err.println("Usage: AlertMigration exportfile");
      return;
    }
    
    try {
      LoggerManager.installLogFactory(new DefaultLogFactory(Level.WARNING));

      AlertMigration migration = new AlertMigration(args[0]);
      //migration.migrate();

      System.out.println("archive.dirname="+migration.props.get("archive.dirname"));
    
    } catch (Exception e) {
      
      System.out.println(e);
      e.printStackTrace();
    }
  }

  public AlertMigration(String exportFile) throws Exception {

    this.exportFile = exportFile;
    initialize();
  }
  
  /**
   * Generates the export file
   * 
   * @return The number of alert definitions exported
   */
  public int migrate() {

    int exported = 0;

    try {

      List<File> metaDataFiles = getMetaDataFiles();
      StringBuilder sb = new StringBuilder();
            
      for (File metaDataFile: metaDataFiles) {

        try {
          String fileName = metaDataFile.getName();

          System.out.println("Generating alert export for " + fileName);

          MetaDataFileReader reader = new MetaDataFileReader(metaDataFile, true);

          NodeMetaDataSummary summary = reader.getSummary();
          NodeMetaData data = reader.getNodeMetaData();

          String pathKeyFromFileName = fileName.substring(0, fileName.indexOf("-"));
          long pathKey = Long.parseLong(pathKeyFromFileName);
          
          // Get the summary's entity key.
          EntityKey entityKey = this.symbolManager.getEntityKeyFromPathKey(pathKey);
          
          if (entityKey != null) {
            summary.setEntityKey(pathKey, entityKey);
          } else {
            continue;
          }

          if (summary != null) {
            System.out.println("Summary: " + summary.toString());
          }

          if (data != null) {
            System.out.println("Data: " + data.toString());
          }
          
          for (INodeMetaDataItem item: data.getAllMetaDataItems()) {
            if (item instanceof Alert) {
              ImportExportManager.appendAlert((Alert) item, summary.getEntityKey(), sb);
              // exportAlert((Alert) item, summary.getEntityKey(), sb);
              exported++;
            }
          }
          
        } catch (Exception e) {
          
          System.out.println("Not pre-9.23 metatdata...");
          continue;          
        }
        
        System.out.println();
      }

      if (exported > 0) {
        writeExportFile(sb.toString());
      } else {
        System.out.println("No metatdata to migrate");        
      }
      
      return exported;

    } catch (Exception e) {
      
      System.out.println(e);
      e.printStackTrace();
      
      return -1;

    } finally {
      if (this.symbolManager != null) {
        this.symbolManager.shutdown();
      }
    }    
  }
  
  private void writeExportFile(String defs) {
  
    try {
      File file = new File(this.exportFile);

      FileWriter writer = new FileWriter(file);
      writer.write(defs);
      
      writer.close();
      
      System.out.println("Alerts exported to " + this.exportFile);

    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }
  }
  
  private List<File> getMetaDataFiles() {
    
    List<File> metaDataFiles = new ArrayList<File>();
    
    try {

      File[] files = new File(this.serverDir + File.separator + "storage"
          + File.separator + "metadata" + File.separator + "Default Client").listFiles();

      if (files != null) {
        metaDataFiles.addAll(Arrays.asList(files));
      }

    } catch (Exception e) {
      System.out.println(e);
      e.printStackTrace();
    }

    return metaDataFiles;
  }

  private void initialize() throws Exception {

    this.serverDir = InstallUtil.getInstallDir(EXAMPLE_PATH).getCanonicalPath();

    props = new DiagProperties(new File(serverDir + File.separator + "etc"
          + File.separator + "server.properties"));
    props.load();

    ByteBufferConfig.initialize(props, new NullLogger());

    Persistence persistence = Persistence.createDumperInstance(props);

    this.symbolManager = persistence.getSymbolManager();
  }

  /** Copied from ImportExportManager.java */
  private void exportAlert(Alert alert, EntityKey entity, StringBuilder sb) {
    
    Metric.Type metricType = alert.getMetricType();
    ArrayList<INodeData> nodePath;
    
    sb.append("alert ");
    
    // Append: metric:"<metric-name>", [type:"<metric-type>", ]
    nodePath = appendMetricInfo(sb, entity, metricType);
   
    // Append: name:"<name>", description:"<description>", etc., 
    sb.append(alert.toGroovyDsl());
    
    // Append: path:"<path-without-metric-data>"
    appendPath(sb, nodePath);
    
    sb.append(", overwrite:\"true\"");
    sb.append("\r\n");
  }

  /** Copied from ImportExportManager.java */
  private final void appendPath(final StringBuilder sb, 
      final ArrayList<INodeData> nodePath) {
    
    sb.append("path:\"/");
    sb.append(GroovyUtil.escapeForScriptBinding(EntityKeyHelper.describe(nodePath, nodePath.size(), (GroupBy)null)));
    sb.append('"');
  }
 
  /** Copied from ImportExportManager.java */
  private final ArrayList<INodeData> appendMetricInfo(final StringBuilder sb, 
      final EntityKey entity, final Metric.Type metricType) {
    
    final ArrayList<INodeData> nodePath = entity.getNodeDataPath();
    int lastNodeIndex = nodePath.size() - 1;
    INodeData nodeData = nodePath.get(lastNodeIndex);
    
    // If the last path element is for status metric data, ...
    if (nodeData instanceof MetricData && "status".equals(nodeData.getName())) {
      // then remove it.
      nodePath.remove(lastNodeIndex);
      nodeData = nodePath.get(--lastNodeIndex);
    }
    // If the last path element is now metric data (which should be always), ...
    if (nodeData instanceof MetricData) {
      // then get the metric name and append it to the string builder.
      final MetricData md = (MetricData)nodePath.get(lastNodeIndex);
      nodePath.remove(lastNodeIndex);
      
      // Append: metric:"<metric-name>", 
      sb.append("metric:\"").append(GroovyUtil.escapeForScriptBinding(md.getName())).append("\", ");
      
      // If we have a non-null, non-average metric type, ...
      if (metricType != null && metricType != Metric.Type.AVERAGE) {
        // then append it as well.
        
        // Append: type:"<metric-type>", 
        sb.append("type:\"").append(metricType.getKey()).append("\", ");
      }
    }
    // Return the entity's node path stripped of its MetricData path elements.
    return nodePath;
  }

  public DiagProperties getProps() {
    return props;
  }
}
