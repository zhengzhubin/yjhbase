package com.yjhbase.etl.jobs.imp;

import com.yjhbase.etl.jobs.imp.functions.ConsumerItemsV2PairFlatMapFunction;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description hive表导入hbase
 **/
public class ConsumerItemsToHbaseV2Job extends AbstractImpJob {
    private static  final Log LOG = LogFactory.getLog(ConsumerItemsToHbaseV2Job.class);
    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
        jvmHost();
    }
    String hql = "select consumerId from tmp.tmp_t_consumers";

    @Override
    public void run(ImpJobOption option) throws Exception{
        HiveToHbaseJobOption jobOption = (HiveToHbaseJobOption) option;

        SparkSession sparkSession = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config(PARAM_YJHBASE_REGION_HFILES_NUMBER, 5)
                .appName("hiveTohbaseJob_consumerItems_" + (System.currentTimeMillis() / 1000))
//                .master("local")
                .enableHiveSupport()
                .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum" , AbstractImpJob.hbaseZookeeper);
        conf.set("zookeeper.znode.parent" , AbstractImpJob.hbaseZnode);
        conf.set(TableOutputFormat.OUTPUT_TABLE, jobOption.getHbaseTablename());
        conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, YjConnectionImplementation.class.getName());
        TableName tn = TableName.valueOf(jobOption.getHbaseTablename());
        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tn), connection.getRegionLocator(tn));

        Integer hfilesPerRegion =
                sparkSession.sparkContext().conf()
                        .getInt(PARAM_YJHBASE_REGION_HFILES_NUMBER, 1);
        Configuration confx = AbstractImpJob.hdfsConfiguration(job.getConfiguration());

        JavaPairRDD<ImmutableBytesWritable, Put> hfileRdd =
                sparkSession.sql(this.hql).javaRDD()
                        .repartition(100) //temp param
                        .flatMapToPair(new ConsumerItemsV2PairFlatMapFunction())
                        .repartitionAndSortWithinPartitions(
                                new HBasePartitioner(connection.getRegionLocator(tn).getStartKeys(), hfilesPerRegion)
                        );

        String hfilePath =
                jobOption.getOutHBaseHdfsPath() == null ?
                        AbstractImpJob.defaultOutHBaseHdfsPath(jobOption.getHbaseTablename()) : jobOption.getOutHBaseHdfsPath();
        hfileRdd.saveAsNewAPIHadoopFile(
                hfilePath,
                ImmutableBytesWritable.class, Put.class,
                HFileOutputFormat2.class, confx);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(hfilePath), connection.getAdmin(), connection.getTable(tn), connection.getRegionLocator(tn));
    }

    public static void main(String... args) throws Exception {
        HiveToHbaseJobOption option = new HiveToHbaseJobOption();
        option.setHbaseTablename("t_consumer_items");
        option.setOutHBaseHdfsPath(AbstractImpJob.defaultOutHBaseHdfsPath(option.getHbaseTablename()));
        ConsumerItemsToHbaseV2Job job = new ConsumerItemsToHbaseV2Job();
        job.run(option);
    }
}
