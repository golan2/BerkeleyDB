package izik.golan.sleepycat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * <pre>
 * <B>Copyright:</B>   HP Software IL
 * <B>Owner:</B>       <a href="mailto:izik.golan@hp.com">Izik Golan</a>
 * <B>Creation:</B>    26/12/13 11:02
 * <B>Since:</B>       BSM 9.21
 * <B>Description:</B>
 *
 * </pre>
 */
public class PersistenceTxtReader {

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        File file = new File("C:\\Users\\golaniz\\Desktop\\QCIM1I92687 (Atali)\\methods.utxt");
        File file2 = new File("C:\\Users\\golaniz\\Desktop\\QCIM1I92687 (Atali)\\methods_sorted.utxt");
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = new BufferedReader(new FileReader(file));
            bw = new BufferedWriter(new FileWriter(file2));

            String line;
            while ((((line = br.readLine()) != null))  )  {
                String[] arr = line.split(";");
                Arrays.sort(arr);
                StringBuilder sb = new StringBuilder();
                for (String s : arr) {
                    sb.append(s).append(";");
                }
                sb.append("\n");
                bw.write(sb.toString());
            }
        }
        finally {
            if (br!=null) {
                br.close();
            }
            if (bw!=null) {
                bw.flush();
                bw.close();
            }

        }

        //fileToSqlServer();

    }

    private static void readTable() throws ClassNotFoundException, SQLException {Connection con = openConnection();


        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM T1");


        while (resultSet.next()) {
            int c1 = resultSet.getInt("C1");
            int c2 = resultSet.getInt("C2");
            System.out.println(c1 + ", " + c2);
        }

        con.close();
    }

    private static void fileToSqlServer() throws ClassNotFoundException, SQLException, IOException {
        Connection con = openConnection();

        con.createStatement().executeUpdate("DELETE FROM [Izik].[dbo].[Persistence]");


        File file = new File("C:\\Users\\golaniz\\Desktop\\QCIM1I92687 (Atali)\\persistence.utxt");
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));

            //int fuse = 0;

            String line;
            while ( /*(fuse++<30) && */(((line = br.readLine()) != null))  )  {

                Line obj = parseLine(line);


                String ps = "INSERT INTO [Izik].[dbo].[Persistence]\n" +
                    "           ([key]\n" +
                    "           ,[lastModified]\n" +
                    "           ,[className]\n" +
                    "           ,[methodName]\n" +
                    "           ,[details])\n" +
                    "     VALUES (?,?,?,?,?)";
                PreparedStatement preparedStatement = con.prepareStatement(ps);
                preparedStatement.setString(1, obj.getKey());
                preparedStatement.setString(2, obj.getLastModified());
                preparedStatement.setString(3, obj.getClassName());
                preparedStatement.setString(4, obj.getMethodName());
                preparedStatement.setString(5, obj.getDetails());
                try {
                    preparedStatement.executeUpdate();
                }
                catch (SQLException e) {
                    System.out.println(obj);
                    e.printStackTrace();
                    throw e;
                }






                //String sql = "INSERT INTO [Izik].[dbo].[Persistence]\n" +
                //    "           ([key]\n" +
                //    "           ,[lastModified]\n" +
                //    "           ,[className]\n" +
                //    "           ,[methodName]\n" +
                //    "           ,[details])\n" +
                //    "     VALUES\n" +
                //    "           ('" + obj.getKey() + "'\n" +
                //    "           ,'" + obj.getLastModified() + "'\n" +
                //    "           ,'" + obj.getClassName() + "'\n" +
                //    "           ,'" + obj.getMethodName() + "'\n" +
                //    "           ,'" + obj.getDetails() + "')";
                //System.out.println(sql);
                //con.createStatement().executeUpdate(sql);



            }

        }
        finally {
            if (br!=null) br.close();
        }
    }

    private static Connection openConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String connectionUrl = "jdbc:sqlserver://labm3amdb21.devlab.ad;" +
            "database=Izik;" +
            "user=sa;" +
            "password=mercurypw";
        return DriverManager.getConnection(connectionUrl);
    }

    private static Line parseLine(String line) {
        int first = line.indexOf(",");
        String key = "";
        if (first>-1) {
            key = safeSubString(line, 0, first);
        }

        int second = line.indexOf(",", first+1);
        String lastModified = "";
        if (second>-1) {
            lastModified = safeSubString(line, first, second);
        }

        int third = line.indexOf(":", second+1);
        String className = "";
        if (third>-1) {
            if ('['==line.charAt(third)) third++;
            className = safeSubString(line, second, third);
        }

        int fourth = line.indexOf(":", third+1);
        String methodName = "";
        if (fourth>-1) {
            methodName = safeSubString(line, third, fourth);
        }

        int fifth = line.indexOf("]", third+1);
        String details = "";
        if (fifth>-1) {
            details = safeSubString(line, fourth, fifth);
        }

        System.out.println("[" + key + "] |~|" + "[" + lastModified + "] |~|" + "[" + className + "] |~|" + "[" + methodName+ "] |~|" + "[" + details + "] |~|" );

        return new Line(key, lastModified, className, methodName, details);

    }

    private static String safeSubString(String line, int first, int second) {
        try {
            String lastModified;
            lastModified = line.substring(first+1, second);
            return lastModified;
        }
        catch (StringIndexOutOfBoundsException e) {
            return "ERROR_ " + e.getMessage() + "  _ERROR";
        }
    }

    private static class Line {
        String key;
        String lastModified;
        String className;
        String methodName;
        String details;

        private Line(String key, String lastModified, String className, String methodName, String details) {
            this.key = key;
            this.lastModified = lastModified;
            this.className = className;
            this.methodName = methodName;
            this.details = details;
        }

        public String getKey() {
            return key.substring(0, Math.min(49,key.length()));
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getLastModified() {
            return lastModified.substring(0, Math.min(49,lastModified.length()));
        }

        public void setLastModified(String lastModified) {
            this.lastModified = lastModified;
        }

        public String getClassName() {
            return className.substring(0, Math.min(3999,className.length()));
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getMethodName() {
            return methodName.substring(0, Math.min(3999,methodName.length()));
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public String getDetails() {
            return details.substring(0, Math.min(3999,details.length()));
        }

        public void setDetails(String details) {
            this.details = details;
        }

        @Override
        public String toString() {
            return "Line{" +
                "key='" + key + '\'' +
                ", lastModified='" + lastModified + '\'' +
                ", className='" + className + '\'' +
                ", methodName='" + methodName + '\'' +
                ", details='" + details + '\'' +
                '}';
        }
    }
}
