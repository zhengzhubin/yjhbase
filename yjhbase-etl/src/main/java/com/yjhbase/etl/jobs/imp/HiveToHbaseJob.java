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
        jvmHost(); //ip & hostname
    }

//    Configuration conf;
    String hql =
            "select item_id, item_name, item_brandname, cname1 " +
                    "from dw.dw_item_info_d where stat_day = '20200221' ";

    public HiveToHbaseJob(ImpJobOption jobOption) {
        super(jobOption);
    }

//    HiveToHbaseJobOption option = null;

    @Override
    public void prepare(){
//        this.option = (HiveToHbaseJobOption) this.getJobOption();
//        System.out.println("optionInfo: " + JSONObject.toJSONString(this.option == null ? new HashMap<>() : this.option));
    }

    @Override
    public void run() throws Exception{
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .config("hbase.zookeeper.quorum", zookeeper)
//                .config("zookeeper.znode.parent", "/yjhbase")
                .appName("hbaseImpJob_hiveToHbase_" + (System.currentTimeMillis() / 1000))
//                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        String zookeeper = "10.0.113.217:2181,10.0.114.255:2181,10.0.112.202:2181";
        conf.set("hbase.zookeeper.quorum" , zookeeper);
        conf.set("zookeeper.znode.parent" , "/yjhbase");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "t_items");
        TableName tn = TableName.valueOf("t_items");
        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tn), connection.getRegionLocator(tn));

        Configuration confx = job.getConfiguration();

        byte[][] startKeys = connection.getRegionLocator(tn).getStartKeys();
        for(byte[] startKey : startKeys) {
            System.out.println("startKey = " + (startKey == null ? "nullKey" : Bytes.toString(startKey)));
        }

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
//        sparkSession.sparkContext().hadoopConfiguration().set("fs.defaultFS", "hdfs://hbasedfs");
//        sparkSession.sparkContext().hadoopConfiguration().set("dfs.nameservices", "hbasedfs");
//        sparkSession.sparkContext().hadoopConfiguration().set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
//        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.113.196:9000");
//        sparkSession.sparkContext().hadoopConfiguration().set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.112.140:9000");
//        sparkSession.sparkContext().hadoopConfiguration().set("dfs.client.failover.proxy.provider.hbasedfs",
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
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

    private void configurationHBaseHdfs(Configuration conf) {
        conf.set("dfs.nameservices", "hbasedfs,nameservice1");
        conf.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn1", "TXIDC63-bigdata-hadoop-namenode1:8020");
        conf.set("dfs.namenode.rpc-address.nameservice1.nn2", "TXIDC64-bigdata-hadoop-namenode2:8020");
        conf.set("dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.113.196:9000");
        conf.set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.112.140:9000");
        conf.set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    }

    public static void main(String... args) throws Exception {
        HiveToHbaseJobOption op = new HiveToHbaseJobOption();
        op.setHbaseTablename("t_items");
        op.setHbaseZookeeper("10.0.113.217:2181,10.0.114.255:2181,10.0.112.202:2181");
        op.setHbaseZnode("yjhbase");

        HiveToHbaseJob job = new HiveToHbaseJob(op);
        job.run();
    }


    private static void jvmHost() {
        String[] kuduNodes = new String[] {
                "10.0.112.140,TXIDC-kudumaidian-cluster5",
                "10.0.113.196,TXIDC-kudumaidian-cluster4",
                "10.0.112.202,TXIDC-kudumaidian-cluster3",
                "10.0.114.255,TXIDC-kudumaidian-cluster2",
                "10.0.113.217,txidc-kudumaidian-cluster1",
                "10.0.40.220,TXIDC417-bigdata-kudu-10",
                "10.0.40.167,TXIDC416-bigdata-kudu-9",
                "10.0.40.207,TXIDC415-bigdata-kudu-8",
                "10.0.40.189,TXIDC414-bigdata-kudu-7",
                "10.0.40.236,TXIDC413-bigdata-kudu-6",
                "10.0.40.248,TXIDC412-bigdata-kudu-5",
                "10.0.40.234,TXIDC411-bigdata-kudu-4",
                "10.0.40.215,TXIDC410-bigdata-kudu-3",
                "10.0.40.158,TXIDC409-bigdata-kudu-2",
                "10.0.40.240,TXIDC408-bigdata-kudu-1"
        };


        Properties virtualDns = new Properties();
        for(String node: kuduNodes) {
            String hostname = node.split(",")[1];
            String ip = node.split(",")[0];
            virtualDns.put(hostname, ip);
            if(!hostname.equals(hostname.toLowerCase())) {
                virtualDns.put(hostname.toLowerCase(), ip);
            }
        }
        JavaHost.updateVirtualDns(virtualDns);
    }
}
