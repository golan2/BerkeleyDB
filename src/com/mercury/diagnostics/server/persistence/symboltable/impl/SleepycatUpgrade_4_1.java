package com.mercury.diagnostics.server.persistence.symboltable.impl;

import com.mercury.diagnostics.common.logging.DefaultLogFactory;
import com.mercury.diagnostics.common.logging.Level;
import com.mercury.diagnostics.common.logging.LoggerManager;
import com.mercury.diagnostics.common.util.DiagProperties;
import com.mercury.diagnostics.common.util.PropertiesUtil;
import com.mercury.diagnostics.server.util.migration.AlertMigration;
import com.mercury.opal.mediator.util.DiagnosticsServer;

import java.io.File;
import java.io.IOException;

/**
 * <pre>
 * <B>Copyright:</B>   HP Software IL
 * <B>Owner:</B>       <a href="mailto:izik.golan@hp.com">Izik Golan</a>
 * <B>Creation:</B>    25/06/2015 15:01
 * <B>Description:</B>
 *
 * </pre>
 */
public class SleepycatUpgrade_4_1 {

  private static final String PERSISTENCY_DIR_PROPERTY = "persistency.rootdir";
  private static final String PERSISTENCY_DIR_DEFAULT = "${archive.dirname}/${mediator.id}/${persistency.dirname}";

  public static void main__(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: AlertMigration exportfile");
      return;
    }

    try {
      LoggerManager.installLogFactory(new DefaultLogFactory(Level.WARNING));

      AlertMigration migration = new AlertMigration(args[0]);
      //migration.migrate();

      System.out.println("archive.dirname="+migration.getProps().get("archive.dirname"));

    } catch (Exception e) {

      System.out.println(e);
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {

    if (args.length<1) {
      System.out.println("Missing parameter server root folder");
    }

    String serverDir = args[0];
    System.out.println(serverDir);

    String configFileName = serverDir + File.separator + "etc" + File.separator + "server.properties";
    File file = new File(configFileName);
    System.out.println(file.getCanonicalPath());
    System.out.println(file.exists());
    DiagProperties props = new DiagProperties(file);
    props.load();

    //first argument should be the folder of the installation.
    File persistenceDir = getPersistenceDir(props);

    System.out.println( persistenceDir.getAbsolutePath() );

    //DbPreUpgrade_4_1.main(arguments);
  }

  private static File getPersistenceDir(PropertiesUtil properties)
  {
    return getDirFromProperties(properties, PERSISTENCY_DIR_PROPERTY, PERSISTENCY_DIR_DEFAULT);
  }


  private static File getDirFromProperties(PropertiesUtil properties, String property, String def) {

    String persistencyDir = properties.getResolvedProperty(property, def);

    File dir = new File(persistencyDir);

    if (!dir.isAbsolute()) {
      dir = new File(DiagnosticsServer.getInstallDir(), persistencyDir);
    }
    return dir;
  }

}
