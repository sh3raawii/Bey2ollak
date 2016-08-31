/**
 * Created by Mostafa on 8/31/16.
 */

import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Part2 {
    private final String dbURL = "jdbc:mysql://localhost/bey2ollakdb?autoReconnect=true&useSSL=false" +
            "&jdbcCompliantTruncation=false";
    private final String user = "bey2ollak";
    private final String password = "password";

    //SQL Commands
    private final String dropLogTable = "DROP TABLE IF EXISTS Log";
    private final String createLogTable = "CREATE TABLE Log(id INT NOT NULL AUTO_INCREMENT,host VARCHAR(100) NOT NULL,"+
            "date_time DATETIME NOT NULL, timezone VARCHAR(5), req VARCHAR(10) NOT NULL, url VARCHAR(100) NOT NULL,"+
            "http_version VARCHAR(10), status SMALLINT(3) UNSIGNED NOT NULL,size INTEGER UNSIGNED, PRIMARY KEY(id))";

    private final String insertCommand = "INSERT INTO Log (host,date_time,timezone,req,url,http_version,status,size)" +
            " VALUES (?,?,?,?,?,?,?,?)";
    private final String visitorsSelectCommand = "SELECT DISTINCT host FROM Log";
    private final String hitsSelectCommand = "SELECT url, COUNT(url) AS hits FROM Log GROUP BY url";
    private final String topHitsSelectCommand = "SELECT url, COUNT(url) AS hits FROM Log GROUP BY url " +
            "ORDER BY COUNT(url) DESC";


    private final String createIndex1 = "CREATE INDEX hostIndex USING HASH ON Log (host)";
    private final String createIndex2 = "CREATE INDEX dateTimeIndex USING BTREE ON Log (date_time)";
    private final String createIndex3 = "CREATE INDEX urlIndex USING HASH ON Log (url)";

    private List<LogEntry> log;

    public void save(){
        try(Connection connection = DriverManager.getConnection(dbURL,user,password);
            PreparedStatement insertStatement = connection.prepareStatement(insertCommand);
            Statement statement = connection.createStatement()){

            LogParser myParser = new LogParser();
            myParser.setLogFilePath("NASA_access_log_Jul95");
            myParser.run();
            log = myParser.getLog();
            System.out.println("Parser has Finished");
            myParser.printReport();

            long dbInsertion;
            long[] timings;

            // No Indexes
            System.out.println("No Custom Indexes yet");
            recreateLogTable(statement);
            dbInsertion = insertData(insertStatement);
            timings = generateFiles(statement);
            printReport(dbInsertion, timings);

            System.out.println("Press Enter to continue....");
            System.in.read();

            //Creating different Indexes and testing them

            //Attempt1
            System.out.println("Attempt1");
//            recreateLogTable(statement);
//            statement.execute(createIndex1);
//            statement.execute(createIndex2);
//            statement.execute(createIndex3);
//            dbInsertion = insertData(insertStatement);
//            timings = generateFiles(statement);
//            printReport(dbInsertion, timings);
//
//            System.out.println("Press Enter to continue....");
//            System.in.read();

            //Attempt2
            System.out.println("Attempt2");
//            recreateLogTable(statement);
//            statement.execute(createIndex1);
//            statement.execute(createIndex3);
//            dbInsertion = insertData(insertStatement);
//            timings = generateFiles(statement);
//            printReport(dbInsertion, timings);
//
//            System.out.println("Press Enter to continue....");
//            System.in.read();

            //Attempt3
            System.out.println("Attempt3");
//            recreateLogTable(statement);
//            statement.execute(createIndex1);
//            statement.execute(createIndex2);
//            dbInsertion = insertData(insertStatement);
//            timings = generateFiles(statement);
//            printReport(dbInsertion, timings);
//
//            System.out.println("Tried all scenario.");

        }catch(SQLException | IOException e){
                e.printStackTrace();
        }
    }

    public long insertData(PreparedStatement statement) throws SQLException {
        SimpleDateFormat mySQLDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startInsertionTime = System.currentTimeMillis();
        for (LogEntry entry : log){

            statement.setString(1,entry.host);
            statement.setString(2,mySQLDateFormat.format(entry.dateTime));
            statement.setString(3,entry.zone);
            statement.setString(4,entry.request);
            statement.setString(5,entry.path);
            statement.setString(6,entry.httpVersion);
            statement.setShort(7,entry.status);
            statement.setLong(8,entry.sizeInBytes);

            statement.execute();
        }
        return (System.currentTimeMillis() - startInsertionTime);
    }

    public void recreateLogTable(Statement statement) throws SQLException {
        statement.execute(dropLogTable);
        statement.execute(createLogTable);
    }

    public long[] generateFiles(Statement statement) throws SQLException {
        long[] results = new long[3];
        long startTime;
        ResultSet set;
        try(FileWriter visitorsFile = new FileWriter("visitors.txt",false);
            FileWriter hitsFile = new FileWriter("hits.txt",false);
            FileWriter topHitsFile = new FileWriter("tophits.txt",false)) {

            startTime = System.currentTimeMillis();
            set = statement.executeQuery(visitorsSelectCommand);
            results[0] = System.currentTimeMillis() - startTime;

            while (set.next()){
                visitorsFile.write(set.getString("host") + "\n");
            }

            startTime = System.currentTimeMillis();
            set = statement.executeQuery(hitsSelectCommand);
            results[1] = System.currentTimeMillis() - startTime;

            while (set.next()){
                hitsFile.write(set.getString("url") + "," + set.getString("hits") + "\n");
            }

            startTime = System.currentTimeMillis();
            set = statement.executeQuery(topHitsSelectCommand);
            results[2] = System.currentTimeMillis() - startTime;

            while (set.next()){
                topHitsFile.write(set.getString("url") + "," + set.getString("hits") + "\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    public void printReport(long dbInsertion, long[] files){
        System.out.println("DB Insertion has Finished in " + dbInsertion );
        System.out.println("Query visitors.txt : " + files[0]);
        System.out.println("Query hits.txt : " + files[1]);
        System.out.println("Query topHits.txt : " + files[2] + "\n");
    }

        public static void main (String[] args){
            Part2 part2 = new Part2();
            part2.save();
    }
}