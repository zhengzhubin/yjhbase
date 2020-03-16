package com.yjhbase.etl.jobs.others;

import com.yjhbase.etl.jobs.imp.functions.ConsumersFlatMapFunction;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description consuemrsId 保存到hbase hdfs
 **/
public class ConsumersToHFileJob implements Serializable {

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
    }

    String hql = "select consumerId from tmp.tmp_t_consumers";

    public void run() {
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName("hiveTohbaseJob_consumerItems_" + (System.currentTimeMillis() / 1000))
//                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        JavaRDD<String> consumersRdd =
                sparkSession.sql(this.hql).javaRDD()
                        .repartition(100)
                        .flatMap(new ConsumersFlatMapFunction());

        sparkSession.sparkContext().hadoopConfiguration().set("fs.defaultFS", "hdfs://nameservice1");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.nameservices", "hbasedfs,nameservice1");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.nameservice1.nn1", "TXIDC63-bigdata-hadoop-namenode1:8020");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.nameservice1.nn2", "TXIDC64-bigdata-hadoop-namenode2:8020");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.43.70:9000");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.43.78:9000");
        sparkSession.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        sparkSession.sparkContext().hadoopConfiguration().set(FileOutputFormat.COMPRESS, "false"); // 不实用压缩
        consumersRdd.saveAsTextFile("hdfs://hbasedfs/etl/others/t_consumers/");
    }

    public static void main(String... args) {
        ConsumersToHFileJob job = new ConsumersToHFileJob();
        job.run();
    }
}
