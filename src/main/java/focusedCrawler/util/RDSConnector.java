package focusedCrawler.util;

import focusedCrawler.target.TargetStorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.StringColumn;
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
import java.util.ArrayList;
import java.util.Properties;

import static tech.tablesaw.api.ColumnType.FLOAT;
import static tech.tablesaw.api.ColumnType.SHORT;
import static tech.tablesaw.api.ColumnType.STRING;
import static tech.tablesaw.api.ColumnType.TEXT;

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
//        RDS_INSTANCE_HOSTNAME = "ache-page-source-downloader-client-maq.cde7zdfhn2sa.us-east-1.rds.amazonaws.com";
//        RDS_INSTANCE_PORT = 3306;
//        REGION_NAME =  "us-east-1";
//        DB_USER = "munaf";
//        DB_PASS = "serverless123";
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

    public static void main(String ... args){
        TargetStorageConfig tsc = new TargetStorageConfig();
        RDSConnector conn = new RDSConnector(tsc, "shopify_190128_homepage","/home/surgo-admin/Downloads/shopify-db-update-190128/");

        try {
            conn.parseCSVMetrics_copy();
            conn.updateAWS();
        }catch (Exception ioe){
            System.out.println(ioe.getMessage());
        }

    }

    public void parseCSVMetrics_copy() throws IOException{
        logger.info("Merging files to update MYSQL");
        String dRPath = dataPath+crawlerId+"/data_monitor/downloadrequests.csv";
        String sMPath = dataPath+crawlerId+"/data_monitor/storagemap.csv";

        ColumnType[] dRTypes = {TEXT,STRING,SHORT,STRING,SHORT,STRING,STRING,FLOAT};
        CsvReadOptions.Builder dRBuilder = CsvReadOptions.builder(dRPath)
                .separator('\t')			// table is tab-delimited
                .header(true)				// header
                .columnTypes(dRTypes);


        ColumnType[] sMTypes = {STRING,STRING};
        CsvReadOptions.Builder sMBuilder = CsvReadOptions.builder(sMPath)
                .separator('\t')			// table is tab-delimited
                .header(true)				// header
                .columnTypes(sMTypes);

        Table downloadRequests = Table.read().csv(dRBuilder.build());
        Table storageMap = Table.read().csv(sMBuilder.build());
        logger.info(downloadRequests.structure().toString());
        logger.info(storageMap.structure().toString());

        Table dfJoined = new DataFrameJoiner(downloadRequests, "original_url").leftOuter(storageMap,true,"original_url");

        // add crawler name everywhere
        StringColumn crawler_name = StringColumn.create("crawler_name");
        ArrayList<String> ids = new ArrayList<>(dfJoined.column("original_url").size());
        for (int i = 0; i < dfJoined.column("original_url").size(); i++){
            ids.add(crawlerId);
        }
        crawler_name.addAll(ids);
        dfJoined.addColumns(crawler_name);
//            logger.info(dfJoined.print(10));

        CsvWriteOptions.Builder dfjBuilder = CsvWriteOptions.builder(dataPath+crawlerId+"/data_monitor/joined.csv");
        DataFrameWriter dfw = new DataFrameWriter(dfJoined);
        dfw.csv(dfjBuilder.build());
    }


    public void parseCSVMetrics() throws IOException{
        logger.info("Preparing aggregated CSV files");
        String dRPath = dataPath+"/data_monitor/downloadrequests.csv";
        String sMPath = dataPath+"/data_monitor/storagemap.csv";

        CsvReadOptions.Builder dRBuilder = CsvReadOptions.builder(dRPath)
                        .separator('\t')			// table is tab-delimited
                        .header(true);				// no header

        CsvReadOptions.Builder sMBuilder = CsvReadOptions.builder(sMPath)
                .separator('\t')			// table is tab-delimited
                .header(true);				// no header

        Table downloadRequests = Table.read().csv(dRBuilder.build());
        Table storageMap = Table.read().csv(sMBuilder.build());
        Table dfJoined = new DataFrameJoiner(downloadRequests, "original_url").leftOuter(storageMap,true,"original_url");

        // add crawler name everywhere
        StringColumn crawler_name = StringColumn.create("crawler_name");
        ArrayList<String> ids = new ArrayList<>(dfJoined.column("original_url").size());
        for (int i = 0; i < dfJoined.column("original_url").size(); i++){
            ids.add(crawlerId);
        }
        crawler_name.addAll(ids);
        dfJoined.addColumns(crawler_name);
//            logger.info(dfJoined.print(10));

        CsvWriteOptions.Builder dfjBuilder = CsvWriteOptions.builder(dataPath+"/data_monitor/joined.csv");
        DataFrameWriter dfw = new DataFrameWriter(dfJoined);
        dfw.csv(dfjBuilder.build());
    }

    public void updateAWS() throws SQLException {
        logger.info("Updating RDS endpoint: "+RDS_INSTANCE_HOSTNAME+":"+RDS_INSTANCE_PORT);
        String updateQuery = "LOAD DATA LOCAL INFILE \""+ dataPath + "/data_monitor/joined.csv\" INTO TABLE ACHECrawls.crawls " +
                "FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (`timestamp`, `original_url`, `num_redirects`, `fetched_url`, `status_code`, `status_message`, `content_type`, `response_time`, `location`, `crawler_name`);";

        //get the connection
        Connection connection = getDBConnectionWithoutIam();

        //verify the connection is successful
        Statement stmt= connection.createStatement();
        ResultSet rs = stmt.executeQuery(updateQuery); // do something with rs?

        logger.info("RDS update complete");
        //close the connection
        stmt.close();
        connection.close();
    }

}
