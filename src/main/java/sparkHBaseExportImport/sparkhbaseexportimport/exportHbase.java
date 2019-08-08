/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkHBaseExportImport.sparkhbaseexportimport;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.hadoop.io.compress.BZip2Codec;

/**
 *
 * @author msumbul
 * @web https://whatsbigdata.be
 * @git https://github.com/michelsumbul
 */
public class exportHbase {

    public static void main(String[] args) {
        exportTable(args[0], args[1], Boolean.valueOf(args[2]));
    }

    public static void exportTable(String tablename, String destination, Boolean compress) {
        SparkConf config = new SparkConf().setAppName("Spark HBase Export table:" + tablename);
        JavaSparkContext jsc = new JavaSparkContext(config);

        JavaPairRDD<ImmutableBytesWritable, Result> data = collectDataFromHBaseTable(jsc, tablename);
        data.repartition(30);
        JavaRDD<String> dataString = data.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {

            @Override
            public String call(Tuple2<ImmutableBytesWritable, Result> t) {
                String out = "";
                Result r = t._2;
                //r.rawCells()[0].getFamily
                Cell[] cells = r.rawCells();
                out = out + Bytes.toString(r.getRow()) + "_SparkHBaseExport_";
                for (Cell c : cells) {

                    out = out + Bytes.toString(c.getFamilyArray()) + "__" + Bytes.toString(c.getQualifierArray()) + "__" + Bytes.toString(c.getValueArray()) + "_data_";
                }
                return out;
            }
        }
        );
        if (!compress) {
            dataString.saveAsTextFile(destination + tablename);
          
        } else {
            dataString.saveAsTextFile(destination + tablename + "_compress", BZip2Codec.class);
        }
        jsc.close();
    }

    public static JavaPairRDD<ImmutableBytesWritable, Result> collectDataFromHBaseTable(JavaSparkContext jsc, String tablename) {
        Configuration confTable = hbaseUtil.getHBaseConfig();
        confTable.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.INPUT_TABLE, tablename);

        JavaPairRDD<ImmutableBytesWritable, Result> dataEvent
                = jsc.newAPIHadoopRDD(confTable, org.apache.hadoop.hbase.mapreduce.TableInputFormat.class, ImmutableBytesWritable.class, Result.class
                );
        return dataEvent;
    }
}
