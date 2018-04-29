package izik.golan.test;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

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
public class BerkeleyUtilOps extends AbsBerkeleyDb {

    //private static final String ENV_PATH = "C:\\Users\\golaniz\\Documents\\Izik\\Java\\Projects\\BerkeleyDB\\bdb\\test1";
    //private static final String DB_NAME = "randomBytes";


    //private static final String ENV_PATH = "C:\\Users\\golaniz\\Desktop\\myd-vm03672\\mediator-MYD-VM03672\\symboltable";
    //private static final String DB_NAME = "localsymboltable";

    //private static final String ENV_PATH = "C:\\Users\\golaniz\\Desktop\\QCIM1I92940\\Backup\\Days";
    //private static final String DB_NAME = "localsymboltable";

    //private static final String ENV_PATH = "C:\\Users\\golaniz\\Desktop\\QCIM1I92687 (Atali)\\BDB";
    //private static final String DB_NAME = "localsymboltable";

    private static final String ENV_PATH = "C:\\Users\\golaniz\\Desktop\\Misc\\QCIM1I108840_LLoyds\\APPLGVIRTUAL2BD_Days\\Days";
    private static final String DB_NAME = "localsymboltable";

    private static final boolean VALIDATE_AND_CLEAN = true;
    protected static final boolean TRANSACTIONAL_ENV = false;


    protected BerkeleyUtilOps() throws DatabaseException {
        super();
        try {
            //listDatabaseNames();

            printFirstRecords(db, 100);

            //insertSomeValues(env, db);

            //readIG(db);

            //listDatabaseNames(env);

            //readKeyValueAndCountTotalSize();

            //env.cleanLog();


        }
        finally {
            try {
                if (db!=null)  db.close();
                if (env!=null) env.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    public static void main(String[] args) throws Exception {
        new BerkeleyUtilOps();
    }



    private static void scavenger(Environment env) throws IOException, DatabaseException {
        //DbScavenger scavenger = new DbScavenger(env, "C:\\Users\\golaniz\\Desktop\\QCIM1I92687 (Atali)\\Scavenger", true, true, true);
        //scavenger.dump();
    }

    private void readKeyValueAndCountTotalSize() throws DatabaseException {
        System.out.println("OP: readKeyValueAndCountTotalSize");
        Transaction txn = null;
        if (TRANSACTIONAL_ENV) {
            txn = env.beginTransaction(null, null);
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Cursor cursor = db.openCursor(txn, null);
        long totalSize = 0;
        int i = 1;
        while (readNext(key, data, cursor)) {

            String k;
            if (key.getSize()==4) {
                k = Integer.toString(fromByteArray(key.getData()));
            }
            else {
                k = new String(key.getData());
            }

            totalSize += data.getSize();

            i++;
            if (i % 5000 == 0) {
                System.out.println("Record ["+i+"] TotalSize ["+totalSize+"]");
                //try {
                //    Thread.sleep(500);
                //}
                //catch (InterruptedException e) {
                //    //noinspection DefaultFileTemplate
                //    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                //}
            }


        }
        System.out.println("END!!\nRecord ["+i+"] TotalSize ["+totalSize+"]");
        cursor.close();
        if (TRANSACTIONAL_ENV) {
            txn.commit();
        }
    }
    private static void printFirstRecords(Database db, int howManyRecords) throws DatabaseException {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Cursor cursor = db.openCursor(null, null);
        int i = 0;
        while (i<howManyRecords && readNext(key, data, cursor)) {
            i++;
            String k;
            if (key.getSize()==4) {
                //so I guess this is an int
                k = Integer.toString(fromByteArray(key.getData()));
            }
            else {
                k = new String(key.getData());
            }

            StringBuilder buf = new StringBuilder();
            String v = new String(data.getData());
            buf.append(i).append(": K=[").append(k).append("] V=[");
            appendByteArrayToStringBuilder(data, buf);
            buf.append("]");
            System.out.println(buf.toString());


        }
        System.out.println("END!!\nRecord ["+i+"]");
        cursor.close();
    }

    private static void appendByteArrayToStringBuilder(DatabaseEntry data, StringBuilder buf) {
        for (int i = 0; i < data.getData().length; i++) {
            byte b = data.getData()[i];
            buf.append(b).append(" ");
        }
    }

    private static void listKeyValuePairs(Environment env, Database db) throws DatabaseException {
        Transaction txn = null;
        if (TRANSACTIONAL_ENV) {
            txn = env.beginTransaction(null, null);
        }

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();

        Cursor cursor = db.openCursor(txn, null);
        int i = 1;
        while (readNext(key, data, cursor)) {
            String k;
            if (key.getSize()==4) {
                k = Integer.toString(fromByteArray(key.getData()));
            }
            else {
                k = new String(key.getData());
            }


            String v = String.valueOf(data.getSize());//new String(data.getData());
            System.out.println(("" + i++) + ": K=[" + k + "] V_Size=[" + v + "]");
        }
        System.out.println("Last record: " + i);
        cursor.close();
        if (TRANSACTIONAL_ENV) {
            txn.commit();
        }
    }

    private static boolean readNext(DatabaseEntry key, DatabaseEntry data, Cursor cursor) throws DatabaseException {
        OperationStatus next = cursor.getNext(key, data, LockMode.DEFAULT);
        if (next != OperationStatus.SUCCESS) {
            System.out.println("readNext - END - no more records - next=["+next+"]");
        }
        return next == OperationStatus.SUCCESS;
    }

    private static void readIG(Database db) throws DatabaseException {Transaction tx = null;
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus success = db.get(tx, new EnhancedDatabaseEntry("ig"), data, LockMode.DEFAULT);
        if (success == OperationStatus.SUCCESS) {
            System.out.println(new String(data.getData()));
        }
        else {
            throw new RuntimeException("Failed to get {ig} key from BDB");
        }
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

    private static void addRandomKeyValue(Database db, Transaction txn) throws DatabaseException {
        byte[] key = convertGuidAsByteArray(UUID.randomUUID());
        byte[] value = new byte[1024];
        Random random = new Random();
        random.nextBytes(value);
        System.out.println("(K,V) = (" + Arrays.toString(key) + "," + Arrays.toString(value) + ")");
        db.put(txn, new EnhancedDatabaseEntry(UUID.randomUUID()),new EnhancedDatabaseEntry(value));
    }

    @Override
    protected String getEnvPath() {
        return ENV_PATH;
    }

    @Override
    protected String getDatabaseName() {
        return DB_NAME;
    }

    @Override
    protected boolean validateAndClean() {
        return VALIDATE_AND_CLEAN;
    }

    @Override
    protected boolean getTransactionalEnv() {
        return TRANSACTIONAL_ENV;
    }


    private static class EnhancedDatabaseEntry extends DatabaseEntry {
        public EnhancedDatabaseEntry(UUID uuid) {
            super(convertGuidAsByteArray(uuid));
        }
        public EnhancedDatabaseEntry(int val) {
            super(intToByteArray(val));
        }
        public EnhancedDatabaseEntry(byte[] data) {
            super(data);
        }
        public EnhancedDatabaseEntry(String str) {
            super(str.getBytes());
        }
    }















    public static byte[] convertGuidAsByteArray(UUID uuid){
        long longOne = uuid.getMostSignificantBits();
        long longTwo = uuid.getLeastSignificantBits();

        return new byte[] {
            (byte)(longOne >>> 56),
            (byte)(longOne >>> 48),
            (byte)(longOne >>> 40),
            (byte)(longOne >>> 32),
            (byte)(longOne >>> 24),
            (byte)(longOne >>> 16),
            (byte)(longOne >>> 8),
            (byte) longOne,
            (byte)(longTwo >>> 56),
            (byte)(longTwo >>> 48),
            (byte)(longTwo >>> 40),
            (byte)(longTwo >>> 32),
            (byte)(longTwo >>> 24),
            (byte)(longTwo >>> 16),
            (byte)(longTwo >>> 8),
            (byte) longTwo
        };
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

}
