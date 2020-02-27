package com.yjhbase.etl.jobs.imp;

import com.yjhbase.etl.jobs.imp.functions.ConsumerItemsPairFlatMapFunction;
import io.leopard.javahost.JavaHost;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description hive表导入hbase
 **/
public class ConsumerItemsHiveTableToHbaseJob extends AbstractImpJob {
    private static  final Log LOG = LogFactory.getLog(ConsumerItemsHiveTableToHbaseJob.class);
    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
        AbstractImpJob.jvmHost();
    }
    String hql = "select consumerId, itemsId from tmp.tmp_t_consumer_items";

    @Override
    public void run(ImpJobOption option) throws Exception{
        HiveToHbaseJobOption jobOption = (HiveToHbaseJobOption) option;

        SparkSession sparkSession = SparkSession.builder()
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config(PARAM_YJHBASE_REGION_HFILES_NUMBER, 3)
                .appName("hiveTohbaseJob_consumerItems_" + (System.currentTimeMillis() / 1000))
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

        Integer hfilesPerRegion =
                sparkSession.sparkContext().conf()
                        .getInt(PARAM_YJHBASE_REGION_HFILES_NUMBER, 1);
        Configuration confx = AbstractImpJob.hdfsConfiguration(job.getConfiguration());

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd =
                sparkSession.sql(this.hql).javaRDD()
                        .flatMapToPair(new ConsumerItemsPairFlatMapFunction())
                        .repartitionAndSortWithinPartitions(
                                new HBasePartitioner(connection.getRegionLocator(tn).getStartKeys(), hfilesPerRegion)
                        );

        String hfilePath =
                jobOption.getOutHBaseHdfsPath() == null ?
                        AbstractImpJob.defaultOutHBaseHdfsPath(jobOption.getHbaseTablename()) : jobOption.getOutHBaseHdfsPath();
        hfileRdd.saveAsNewAPIHadoopFile(
                hfilePath,
                ImmutableBytesWritable.class, KeyValue.class,
                HFileOutputFormat2.class, confx);

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(hfilePath), connection.getAdmin(), connection.getTable(tn), connection.getRegionLocator(tn));
    }

    public static void main(String... args) throws Exception {
        HiveToHbaseJobOption option = new HiveToHbaseJobOption();
        option.setHbaseTablename("t_consumer_items");
        option.setOutHBaseHdfsPath(AbstractImpJob.defaultOutHBaseHdfsPath(option.getHbaseTablename()));
        ConsumerItemsHiveTableToHbaseJob job = new ConsumerItemsHiveTableToHbaseJob();
        job.run(option);
    }
}
