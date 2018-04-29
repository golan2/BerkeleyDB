package izik.golan.sleepycat;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.util.DbDump;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

/**
 * <pre>
 * <B>Copyright:</B>   Izik Golan
 * <B>Owner:</B>       <a href="mailto:golan2@hotmail.com">Izik Golan</a>
 * <B>Creation:</B>    25/12/13 00:12
 * <B>Since:</B>       BSM 9.21
 * <B>Description:</B>
 *
 * </pre>
 */
public class TestOpenDatabase {
  private static final String      DB_FOLDER    = "C:\\Users\\golaniz\\Desktop\\Misc\\QCCR1I108397 - sleepycat upgrade\\myd-vm05582\\C\\MercuryDiagnostics\\Server\\archive\\mediator-myd-vm05582\\symboltable";

  private static final Strategy    strategy     = Strategy.READ_ALL;
  private static final String      DIAG_DB_NAME = "localsymboltable";
  private static       Environment env;
  private static       Database    db;


  public static void main(String[] args) throws DatabaseException, IOException {

    try {

      env = setEnvironment();

      db = openDatabase();

      System.out.println("env="+env);
      System.out.println("db="+db);

      //allowCreate(env, db);

      listKeyValuePairs();

      //dump();


    }
    finally {
      try {
        if (db !=null)  db.close();
        if (env !=null) env.close();
      }
      catch (DatabaseException e) {
        e.printStackTrace();
      }
    }


  }

  private static void dump() throws IOException, DatabaseException {
    if (env!=null || db!=null) throw new InvalidStateException("The dump performs all it needs. do not open env or db!");
    env = setEnvironment();
    String folder = "C:\\Users\\golaniz\\Documents\\Izik\\Java\\Projects\\BerkeleyDB\\out";
    PrintStream outputFile = new PrintStream(new FileOutputStream(folder + "\\dump.utxt"));
    //JE6
    //DbDump dump = new DbDump(env, DIAG_DB_NAME, outputFile, true);
    DbDump dump = new DbDump(env, DIAG_DB_NAME, outputFile, folder, true);
    dump.dump();
  }

  private static void listKeyValuePairs() throws DatabaseException {
    Transaction txn = null;


    DatabaseEntry key = new DatabaseEntry();
    DatabaseEntry data = new DatabaseEntry();

    Cursor cursor = null;
    try {
      if (strategy.isUseTransaction()) {
        txn = env.beginTransaction(null, null);
      }


      cursor = db.openCursor(txn, null);
      int i = 1;

      boolean moreToRead = readNext(key, data, cursor);
      boolean maxRecordReached = strategy.maxRecordReached(i);

      while ( !maxRecordReached && moreToRead  ) {

        String k;
        if (key.getSize()==4) {
          k = Integer.toString(fromByteArray(key.getData()));
        }
        else {
          k = new String(key.getData());
        }


        String v = String.valueOf(data.getSize());      //new String(data.getData());
        System.out.println(("" + i++) + ": K=[" + k + "] V_Size=[" + v + "]");


        moreToRead = readNext(key, data, cursor);
        maxRecordReached = strategy.maxRecordReached(i);

      }

      if (maxRecordReached) {
        System.out.println("Stopped reading after [" + strategy.getMaxRecordsToRead() + "] records.");
      }


    }
    finally {
      if (cursor!=null) {
        cursor.close();
      }
      if (strategy.isUseTransaction()) {
        txn.commit();
      }

    }
  }

  private static boolean readNext(DatabaseEntry key, DatabaseEntry data, Cursor cursor) throws DatabaseException {
    OperationStatus next = cursor.getNext(key, data, LockMode.DEFAULT);
    if (next == OperationStatus.NOTFOUND) {
      return false;
    }
    else if (next != OperationStatus.SUCCESS) {
      System.out.println("[readNext Op Failed] "+next);
    }
    return next == OperationStatus.SUCCESS;
  }

  private static void insertSomeValues(Environment env, Database db) {
    Transaction tx = null;
    try {
      tx = env.beginTransaction(null, null);

      db.put(tx, new EnhancedDatabaseEntry("ig"), new EnhancedDatabaseEntry("Izik Golan"));
      db.put(tx, new EnhancedDatabaseEntry("lg"),new EnhancedDatabaseEntry("Liat Golan"));
      db.put(tx, new EnhancedDatabaseEntry("sg"), new EnhancedDatabaseEntry("Shaked Golan"));

      tx.commit();
    }
    catch (DatabaseException e) {
      e.printStackTrace();
      try {
        if (tx!=null)  tx.abort();
      }
      catch (DatabaseException e1) {
        e1.printStackTrace();
      }
    }
  }

  private static Database openDatabase() throws DatabaseException {
    DatabaseConfig dbConfig = new DatabaseConfig();
    dbConfig.setAllowCreate(strategy.isAllowCreate());
    dbConfig.setTransactional(strategy.isUseTransaction());
    dbConfig.setSortedDuplicates(strategy.isDuplicatesAllowed());
    return env.openDatabase(null, strategy.getDatabaseName(), dbConfig);
  }

  private static Environment setEnvironment() throws DatabaseException {
    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setConfigParam("je.cleaner.minFileUtilization", "50");
    envConfig.setConfigParam("je.cleaner.minUtilization", "50");
    envConfig.setConfigParam("je.log.checksumRead", "false");
    envConfig.setAllowCreate(strategy.isAllowCreate());
    envConfig.setTransactional(strategy.isUseTransaction());
    //JE6
    //envConfig.setRecoveryProgressListener(new ProgressListener<RecoveryProgress>() {
    //    @Override
    //    public boolean progress(RecoveryProgress phase, long n, long total) {
    //        System.out.println(phase + " [" + n + "," + total + "]");
    //        return true;
    //    }
    //});
    return new Environment(new File(strategy.getEnvironmentPath()), envConfig);
  }

  private static class EnhancedDatabaseEntry extends DatabaseEntry {
    public EnhancedDatabaseEntry(String str) {
      super(str.getBytes());
    }
  }


  private static byte[] intToByteArray(int value) {
    byte[] key = new byte[4];
    key[0] = (byte) value;
    key[1] = (byte) (value >> 8);
    key[2] = (byte) (value >> 16);
    key[3] = (byte) (value >> 24);
    return key;
  }

  private static int fromByteArray(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  private static class Strategy {
    private final String  environmentPath;
    private final String  databaseName;
    private final boolean useTransaction;
    private final boolean duplicatesAllowed;
    private final boolean allowCreate;
    private final int     maxRecordsToRead;

    private Strategy(String environmentPath, String databaseName, boolean useTransaction, boolean duplicatesAllowed, boolean allowCreate, int maxRecordsToRead) {
      this.environmentPath = environmentPath;
      this.databaseName = databaseName;
      this.useTransaction = useTransaction;
      this.duplicatesAllowed = duplicatesAllowed;
      this.allowCreate = allowCreate;
      this.maxRecordsToRead = maxRecordsToRead;
    }

    private String getEnvironmentPath() {
      return environmentPath;
    }

    private String getDatabaseName() {
      return databaseName;
    }

    private boolean isUseTransaction() {
      return useTransaction;
    }

    private boolean isDuplicatesAllowed() {
      return duplicatesAllowed;
    }

    private boolean isAllowCreate() {
      return allowCreate;
    }

    private int getMaxRecordsToRead() {
      return maxRecordsToRead;
    }

    private boolean maxRecordReached(int i) {
      return maxRecordsToRead != -1 && (i > maxRecordsToRead);
    }

    private static final Strategy READ_ALL = new Strategy(DB_FOLDER,
                                                          DIAG_DB_NAME,
                                                          false,
                                                          false,
                                                          false,
                                                          -1
    );

    private static final Strategy READ_TOP_THOUSAND = new Strategy(DB_FOLDER,
                                                                   DIAG_DB_NAME,
                                                                   false,
                                                                   false,
                                                                   false,
                                                                   1000
    );

    private static final Strategy RECOVER = new Strategy(DB_FOLDER,
                                                         DIAG_DB_NAME,
                                                         false,
                                                         false,
                                                         true,
                                                         -1
    );

    private static final Strategy LOCAL_TEST = new Strategy("C:\\Users\\golaniz\\Documents\\Izik\\Java\\Projects\\BerkeleyDB\\bdb\\test1",
                                                            "randomBytes",
                                                            true,
                                                            true,
                                                            true,
                                                            -1
    );
  }


}
