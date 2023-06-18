package org.example;


import java.util.Arrays;
import java.util.Map;

import com.datastax.bdp.spark.DseSparkConfHelper;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import com.datastax.spark.connector.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkCassandraConnector
{
    public JavaSparkContext getSparkContext()
    {
        // Set up the DSE Spark configuration
        SparkConf conf = DseSparkConfHelper.enrichSparkConf(new SparkConf())
                .setAppName("DseExample")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        // Create a Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;
    }

    public void getCassandraTableRows(JavaSparkContext sc)  {

        // Read data from DSE table
        CassandraTableScanJavaRDD<CassandraRow> rdd = CassandraJavaUtil.javaFunctions(sc)
                .cassandraTable("customer_test", "insight_ts")
                .select("tag_id", "data_quality", "event_time", "event_value")
                .where("event_value > 113.00");

        // Print the data to the console
        rdd.foreach(row -> {
            int column1Value = row.getInt("tag_id");
            int column2Value = row.getInt("data_quality");
            int column3Value = row.getInt("event_time");
            System.out.println("column1Value: " + column1Value);
            System.out.println("column2Value: " + column2Value);
            System.out.println("column3Value: " + column3Value);
        });

        // Close the Spark context
        sc.stop();
    }
}


