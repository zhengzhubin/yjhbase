package com.yjhbase.etl.jobs.imp;

import io.leopard.javahost.JavaHost;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.net.URI;
import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description hive表导入hbase
 **/
public class HiveToHbaseJob extends AbstractImpJob {

    private static  final Log LOG = LogFactory.getLog(HiveToHbaseJob.class);

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
        AbstractImpJob.jvmHost();
    }

    String hql =
            "select item_id, item_name, item_brandname, cname1 " +
                    "from dw.dw_item_info_d where stat_day = '20200221' ";

    @Override
    public void run(ImpJobOption option) throws Exception {
        HiveToHbaseJobOption jobOption = (HiveToHbaseJobOption) option;
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .appName("hbaseImpJob_hiveToHbase_" + (System.currentTimeMillis() / 1000))
//                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum" , AbstractImpJob.hbaseZookeeper);
        conf.set("zookeeper.znode.parent" , AbstractImpJob.hbaseZnode);
        conf.set(TableOutputFormat.OUTPUT_TABLE, jobOption.getHbaseTablename());
        TableName tn = TableName.valueOf(jobOption.getHbaseTablename());
        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tn), connection.getRegionLocator(tn));

        Configuration confx = job.getConfiguration();
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd =
                sparkSession.sql(this.hql).javaRDD()
                        .flatMapToPair(new PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>() {
                            @Override
                            public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
                                Integer itemId = row.getAs("item_id");
                                if(itemId == null) itemId = 0;
                                String itemName = row.getAs("item_name");
                                String brandName = row.getAs("item_brandname");
                                String cname1 = row.getAs("cname1");
                                String rowkey = String.format("%02d", Math.abs(itemId) % 100) + "::" +itemId;
                                ImmutableBytesWritable rkBytes =
                                        new ImmutableBytesWritable(Bytes.toBytes(rowkey));
                                List<Tuple2<ImmutableBytesWritable, KeyValue>> retKVs = new ArrayList<>();
                                if(StringUtils.isNotBlank(brandName)) {
                                    retKVs.add(new Tuple2<>(rkBytes,
                                            new KeyValue(
                                                    Bytes.toBytes(rowkey),
                                                    Bytes.toBytes("f"),
                                                    Bytes.toBytes("brandName"), Bytes.toBytes(brandName))));
                                }
                                if(StringUtils.isNotBlank(cname1)) {
                                    retKVs.add(new Tuple2<>(rkBytes,
                                            new KeyValue(
                                                    Bytes.toBytes(rowkey),
                                                    Bytes.toBytes("f"),
                                                    Bytes.toBytes("cname1"), Bytes.toBytes(cname1))));
                                }
                                retKVs.add(new Tuple2<>(rkBytes,
                                        new KeyValue(
                                                Bytes.toBytes(rowkey),
                                                Bytes.toBytes("f"),
                                                Bytes.toBytes("itemId"), Bytes.toBytes(itemId))));
                                if(StringUtils.isNotBlank(itemName)) {
                                    retKVs.add(new Tuple2<>(rkBytes,
                                            new KeyValue(
                                                    Bytes.toBytes(rowkey),
                                                    Bytes.toBytes("f"),
                                                    Bytes.toBytes("itemName"), Bytes.toBytes(itemName))));
                                }
                                return retKVs.iterator();
                            }
                        }).repartitionAndSortWithinPartitions(new HBasePartitioner(connection.getRegionLocator(tn).getStartKeys()));

        confx.set("fs.defaultFS", "hdfs://nameservice1");
        confx.set("dfs.nameservices", "hbasedfs,nameservice1");
        confx.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        confx.set("dfs.namenode.rpc-address.nameservice1.nn1", "TXIDC63-bigdata-hadoop-namenode1:8020");
        confx.set("dfs.namenode.rpc-address.nameservice1.nn2", "TXIDC64-bigdata-hadoop-namenode2:8020");
        confx.set("dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        confx.set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.113.196:9000");
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.112.140:9000");
        confx.set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        String hfilePath = "hdfs://hbasedfs/etl/imp/t_items";
        hfileRdd.saveAsNewAPIHadoopFile(
                hfilePath,
                ImmutableBytesWritable.class, KeyValue.class,
                HFileOutputFormat2.class, confx);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(hfilePath), connection.getAdmin(), connection.getTable(tn), connection.getRegionLocator(tn));
    }

    public static void main(String... args) throws Exception {
        HiveToHbaseJobOption option = new HiveToHbaseJobOption();
        option.setHbaseTablename("t_items");

        HiveToHbaseJob job = new HiveToHbaseJob();
        job.run(option);
    }
}
