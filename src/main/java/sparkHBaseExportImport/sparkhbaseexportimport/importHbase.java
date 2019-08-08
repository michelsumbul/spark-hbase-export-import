/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkHBaseExportImport.sparkhbaseexportimport;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 *
 * @author msumbul
 * @web https://whatsbigdata.be
 * @git https://github.com/michelsumbul
 */
public class importHbase {

    public static void main(String[] args) {
        imporTable(args[0], args[1]);
    }

    public static void imporTable(String tablename, String Source) {

        try {
            SparkConf config = new SparkConf().setAppName("Spark HBase Import table: " + tablename);
            JavaSparkContext jsc = new JavaSparkContext(config);
            JavaRDD<String> data = jsc.textFile(Source);

            Configuration conf = hbaseUtil.getHBaseConfig();
            conf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, tablename);

            Job newAPIJobConfiguration1 = Job.getInstance(conf);
            newAPIJobConfiguration1.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tablename);
            newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = data.mapToPair(
                    new PairFunction<String, ImmutableBytesWritable, Put>() {

                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(String t) {

                    String[] splitRowkey = t.split("_SparkHBaseExport_");

                    String id = splitRowkey[0];
                    String[] splitData = splitRowkey[1].split("_data_");
                    Put put = new Put(Bytes.toBytes(id));
                    for (String data : splitData) {
                        String[] dataToInsert = data.split("__");
                        if (dataToInsert.length == 3) {
                           
                            put.addColumn(Bytes.toBytes(dataToInsert[0]), Bytes.toBytes(dataToInsert[1]), Bytes.toBytes(dataToInsert[2]));
                        }
                    }
                    return new Tuple2<>(new ImmutableBytesWritable(id.getBytes()), put);
                }
            });

            hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration());
            jsc.close();
        } catch (IOException ex) {
            Logger.getLogger(importHbase.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
