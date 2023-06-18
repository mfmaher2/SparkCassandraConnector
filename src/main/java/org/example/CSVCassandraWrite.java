package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.spark.sql.Row;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

public class CSVCassandraWrite {
    public static void main(String args[]) {
//        SparkCassandraConnector sparkCassandraConnector = new SparkCassandraConnector();
//        sparkCassandraConnector.getCassandraTableRows(sparkCassandraConnector.getSparkContext());

        SparkSession spark = SparkSession.builder()
                .appName("Spark Cassandra Job")
                .master("local") // Use this for local mode
                //.master("spark://masterURL:7077") // Use this for a standalone cluster
                .getOrCreate();

//        SparkContext sContext = spark.sparkContext();
//        SparkConf sc = sContext.getConf();


//        spark.conf.set("spark.cassandra.input.split.size_in_mb", "64") // May need to tune
//        spark.conf.set("spark.cassandra.output.concurrent.writes", "10") // May need to tune
//        spark.conf.set("spark.cassandra.output.batch.size.rows", "auto") // May need to tune
//        spark.conf.set("spark.cassandra.output.batch.size.bytes", "auto") // May need to tune

        // Parent directory containing subdirectories with CSV files
//        String parentDirectoryPath = Util.getSparkProp(sc, "spark.source.parentDirectoryPath");
        String parentDirectoryPath = "/Users/mike.maher/Documents/dev/s3/";
        // Create a File object for the root directory
        File rootDirectory = new File(parentDirectoryPath);

        // Start recursive traversal from the root directory
        processSubdirectories(rootDirectory, spark);

//        // Create the CqlSession object:
//        try (CqlSession session = CqlSession.builder()
//                .withCloudSecureConnectBundle(Paths.get("/Users/mike.maher/Downloads/secure-connect-zdmtarget.zip"))
//                .withAuthCredentials("BxnhqKUWqjAlCkmaUnDTQWZt","uWl+ki_TIofAJZT0Um055dH,w9kZeqUdEWAgDeecKNTJohfS8aOUiB3u-3zj7vjts8yqqZiIftvrkaTE.Afut._BMihlA5v-BqQimhhOcY_CkmTzAwkY8jluZNtLOZ6t")
//                .build()) {


//                    Dataset df = spark.read().option("header", "true")
//                            .csv("file:////Users/mike.maher/Documents/dev/s3/file0.csv/part-00000-1db3042d-eb45-40f6-8c42-e910d7267f6c-c000.csv");



//                System.out.println("Data inserted from " + csvFile.getName() + " into Cassandra.");
            }

    private static void processSubdirectories(File parentDirectory, SparkSession spark) {
        File[] subdirectories = parentDirectory.listFiles(File::isDirectory);
        if (subdirectories != null) {
            for (File subdirectory : subdirectories) {
                File[] csvFiles = subdirectory.listFiles((dir, name) -> name.endsWith(".csv"));
                if (csvFiles != null) {
                    for (File csvFile : csvFiles) {
                        // Load CSV file into DataFrame
                        System.out.println("Processing file: " + csvFile.getAbsolutePath());
                        Dataset df = spark.read().option("header", "true").csv(csvFile.getAbsolutePath());

                        Dataset dfWithYearMonth = df.withColumn("yyyy_mm", concat(year(col("event_time")),
                                functions.format_string("%02d", month(col("event_time")))));

                        dfWithYearMonth.toJavaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
                            @Override
                            public void call(Iterator<Row> t) throws Exception {

                                String destinationScbPath = "/Users/mike.maher/Documents/dev/demo/secure-connect-ks1.zip"; //Util.getSparkPropOrEmpty(sc, "spark.destination.scb");
//                                String destinationHost = Util.getSparkPropOrEmpty(sc, "spark.destination.host");
//                                String destinationPort = Util.getSparkPropOr(sc, "spark.destination.port", "9042");
                                String destinationUsername = "EhvMAqaFIPODpJYqQZGrdewd"; //Util.getSparkProp(sc, "spark.destination.username");
                                String destinationPassword = "xhSHKoTizqIbL+1XRTCDJj3jrlP,RKDykAyYMspwZm6X-T11l69_,,1DbibdFplGrcOhP2BlS7T-7BAwReAOyc7lWZRIOCpDjpe,UYycsKBKfN_4HkWYrSLR-IcI+HlX"; //Util.getSparkProp(sc, "spark.destination.password");
                                String astraKeyspaceTable = "basic.insight_ts_new"; //Util.getSparkProp(sc, "spark.destination.keyspaceTable");

                                // Create the CqlSession object:
                                try (CqlSession session = CqlSession.builder()
                                        .withCloudSecureConnectBundle(Paths.get(destinationScbPath))
                                        .withAuthCredentials(destinationUsername, destinationPassword)
                                        .build()) {

                                    String cql = "INSERT INTO " + astraKeyspaceTable + " (tag_id, yyyymm, data_quality, event_time, event_value) VALUES (?, ?, ?, ?, ?)";
                                    PreparedStatement prepared = session.prepare(cql);

                                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

                                    while (t.hasNext()) {

                                        Row row = t.next();

                                        String tag_id = row.getAs("tag_id");
                                        String yyyymmStr = row.getAs("yyyy_mm");
                                        Integer yyyymm = null;
                                        if (yyyymmStr != null && yyyymmStr.matches("\\d+")) {
                                            yyyymm = Integer.parseInt(yyyymmStr);
                                        }

                                        String data_qualityStr = row.getAs("data_quality");
                                        Integer data_quality = null;
                                        if (data_qualityStr != null && data_qualityStr.matches("\\d+")) {
                                            data_quality = Integer.parseInt(data_qualityStr);
                                        }

                                        String event_timeStr = row.getAs("event_time");
                                        Instant event_time = null;
                                        if (event_timeStr != null) {
                                            LocalDateTime localDateTime = LocalDateTime.parse(event_timeStr, formatter);
                                            event_time = localDateTime.atZone(ZoneOffset.UTC).toInstant();
                                        }

                                        Double event_value = null;
                                        String event_valueStr = row.getAs("event_value");
                                        if (event_valueStr != null && event_valueStr.matches("-?\\d+(\\.\\d+)?")) {
                                            event_value = Double.parseDouble(event_valueStr);
                                        }

                                        BoundStatement bound = prepared.bind(tag_id, yyyymm, data_quality, event_time, event_value);
                                        System.out.println("cql: " + bound);
                                        session.execute(bound);
                                    }

                                    session.close();
                                }
                            }
                        });
                    }
                }
                // Recursively process subdirectories
                processSubdirectories(subdirectory, spark);
            }
        }
    }


}








