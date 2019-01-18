package focusedCrawler.util;

import focusedCrawler.target.TargetStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;
import tech.tablesaw.io.DataFrameWriter;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvWriteOptions;
import tech.tablesaw.joining.DataFrameJoiner;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class RDSConnector {

    private static Logger logger = LoggerFactory.getLogger(RDSConnector.class);
    private final TargetStorageConfig config;
    private final String crawlerId;
    private final String dataPath;

    //Configuration parameters for the generation of the IAM Database Authentication token
    private final String RDS_INSTANCE_HOSTNAME;
    private final int RDS_INSTANCE_PORT;
    private final String REGION_NAME;
    private final String DB_USER;
    private final String JDBC_URL;
    private final String DB_PASS;

    public RDSConnector(TargetStorageConfig config, String crawlerId, String dataPath) {
        this.config = config;
        this.crawlerId = crawlerId;
        this.dataPath = dataPath;
        RDS_INSTANCE_HOSTNAME = config.getDbEndpoint();
        RDS_INSTANCE_PORT = config.getDbPort();
        REGION_NAME =  config.getDbRegion();
        DB_USER = config.getDbUser();
        DB_PASS = config.getDbPass();
        JDBC_URL = "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT;
    }

    /**
     * This method returns a connection to the db instance authenticated without IAM Database Authentication
     * @return
     * @throws SQLException
     */
    private Connection getDBConnectionWithoutIam() throws SQLException{
        return DriverManager.getConnection(JDBC_URL, setMySqlConnectionProperties());
    }

    /**
     * This method sets the mysql connection properties which excludes the IAM Database Authentication token
     * as the password. It also specifies that SSL verification is not required.
     * @return
     */
    private Properties setMySqlConnectionProperties() {
        Properties mysqlConnectionProperties = new Properties();
        mysqlConnectionProperties.setProperty("verifyServerCertificate","false");
        mysqlConnectionProperties.setProperty("useSSL", "false");
        mysqlConnectionProperties.setProperty("user", DB_USER);
        mysqlConnectionProperties.setProperty("password", DB_PASS);
        return mysqlConnectionProperties;
    }

    public void parseCSVMetrics() throws IOException{
        logger.info("Merging files to update MYSQL");
        String dRPath = dataPath+crawlerId+"/data_monitor/downloadrequests.csv";
        String sMPath = dataPath+crawlerId+"/data_monitor/storagemap.csv";

        CsvReadOptions.Builder dRBuilder = CsvReadOptions.builder(dRPath)
                        .separator('\t')			// table is tab-delimited
                        .header(false);				// no header

        CsvReadOptions.Builder sMBuilder = CsvReadOptions.builder(sMPath)
                .separator('\t')			// table is tab-delimited
                .header(false);				// no header

            Table downloadRequests = Table.read().csv(dRBuilder.build());
            Table storageMap = Table.read().csv(sMBuilder.build());
            Table dfJoined = new DataFrameJoiner(downloadRequests, "C5").leftOuter(storageMap,true,"C0");
//            logger.info(dfJoined.print(10));

            CsvWriteOptions.Builder dfjBuilder = CsvWriteOptions.builder(dataPath+crawlerId+"/data_monitor/joined.csv");
            DataFrameWriter dfw = new DataFrameWriter(dfJoined);
            dfw.csv(dfjBuilder.build());
    }

    public void updateAWS() throws SQLException {
        logger.info("Updating RDS endpoint: "+RDS_INSTANCE_HOSTNAME+":"+RDS_INSTANCE_PORT);
        String updateQuery = "LOAD DATA LOCAL INFILE \""+ dataPath + crawlerId +"/data_monitor/joined.csv\" INTO TABLE ACHECrawls.crawls " +
                "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (`Timestamp`, `Content_Type`, `Status_Code`, `Status_Message`, `Response_time`, `Crawl_URL`, `Num_redirects`, `Fetched_URL`, `Location`,`Host_IP`);";

        //get the connection
        Connection connection = getDBConnectionWithoutIam();

        //verify the connection is successful
        Statement stmt= connection.createStatement();
        ResultSet rs=stmt.executeQuery(updateQuery); // do something with rs?

        //close the connection
        stmt.close();
        connection.close();
    }

}
