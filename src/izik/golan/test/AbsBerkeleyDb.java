package izik.golan.test;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.util.List;

/**
 * <pre>
 * <B>Copyright:</B>   HP Software IL
 * <B>Owner:</B>       <a href="mailto:izik.golan@hp.com">Izik Golan</a>
 * <B>Creation:</B>    31/12/13 09:09
 * <B>Since:</B>       BSM 9.21
 * <B>Description:</B>
 *
 * </pre>
 */
public abstract class AbsBerkeleyDb {


    protected Environment env = null;
    protected Database db = null;

    protected AbsBerkeleyDb() throws DatabaseException {
        setEnvironment();
        openDatabase();
    }

    protected void setEnvironment() throws DatabaseException {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        if (validateAndClean()) {
            envConfig.setConfigParam("je.cleaner.minFileUtilization", "50");
            envConfig.setConfigParam("je.cleaner.minUtilization", "50");
        }
        this.env = new Environment(new File(getEnvPath()), envConfig);
        System.out.println("ENV OPEN: " + getEnvPath());
    }

    protected void listDatabaseNames() throws DatabaseException {
        
        List names = env.getDatabaseNames();
        System.out.println("listDatabaseNames (size="+names.size()+"):");
        for (Object o : names) {
            System.out.println(o);
        }
    }

    protected void openDatabase() throws DatabaseException {
        DatabaseConfig dbConfig = new DatabaseConfig();
        //dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(getTransactionalEnv());
        //dbConfig.setSortedDuplicates(true);
        this.db = env.openDatabase(null, getDatabaseName(), dbConfig);
        System.out.println("DB OPEN: " + getDatabaseName());
    }

    protected abstract boolean validateAndClean();

    protected abstract boolean getTransactionalEnv();

    protected abstract String getEnvPath();

    protected abstract String getDatabaseName();


}
