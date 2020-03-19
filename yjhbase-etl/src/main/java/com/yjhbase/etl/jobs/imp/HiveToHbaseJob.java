package com.yjhbase.etl.jobs.imp;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.yjhbase.etl.dto.RkColumn;
import com.yjhbase.etl.jobs.imp.functions.ConsumerItemsV2PairFlatMapFunction;
import com.yjhbase.etl.jobs.imp.functions.HiveToHBasePairFlatMapFunction;
import com.yjhbase.etl.utils.ParseCommand;
import io.leopard.javahost.JavaHost;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description hive表导入hbase
 **/
public class HiveToHbaseJob extends AbstractImpJob {

    private static  final Log LOG = LogFactory.getLog(HiveToHbaseJob.class);

    private static final String ETL_JOBS_HDFS_ROOTDIR = "/etl/hbase/jobs/";
    private static final Long DEFAULT_SPLIT_MINSIZE = 512L * 1024 * 1024; //512mb
//    private static final String PARAM_SPARK_ETL_JOBID = "spark.etl.hbase.hive.jobid";
//    private String jobId;

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
        AbstractImpJob.jvmHost();
    }

    @Override
    public void run(String... args) throws Exception {

        HiveToHbaseJobOption jobOption = this.buildJobOptionInfo(args);

        SparkSession.Builder builder = SparkSession.builder();
        builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.dynamicAllocation.enabled", false)
//                .config("spark.dynamicAllocation.minExecutors", 2)
//                .config("spark.dynamicAllocation.maxExecutors", 8)
                .config(FileInputFormat.SPLIT_MINSIZE, DEFAULT_SPLIT_MINSIZE)
                .config("spark.driver.cores", jobOption.getNumDriverCores())
                .config("spark.driver.memory", jobOption.getGbOfDriverMemory() + "g")
                .config("spark.executor.instances", jobOption.getNumExecutors())
                .config("spark.executor.memory", jobOption.getGbOfExecutorMemory() + "g")
                .config("spark.executor.cores", jobOption.getNumExecutorCores());

        SparkSession sparkSession =
                builder.appName("etl_hiveTohbase_" + (System.currentTimeMillis() / 1000))
//                .master("local")
                        .enableHiveSupport()
                        .getOrCreate();

        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM , AbstractImpJob.hbaseZookeeper);
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT , AbstractImpJob.hbaseZnode);
        conf.set(TableOutputFormat.OUTPUT_TABLE, jobOption.getHbaseTablename());
        conf.set(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL, YjConnectionImplementation.class.getName());
        TableName tn = TableName.valueOf(jobOption.getHbaseTablename());
        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tn), connection.getRegionLocator(tn));

        Configuration confx = AbstractImpJob.hdfsConfiguration(job.getConfiguration());
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hfileRdd =
                sparkSession.sql(jobOption.getSparkSql()).javaRDD()
//                        .repartition(400) //temp param
                        .flatMapToPair(new HiveToHBasePairFlatMapFunction(jobOption))
                        .repartitionAndSortWithinPartitions(
                                new HBasePartitioner(connection.getRegionLocator(tn).getStartKeys(), jobOption.getNumberOfFilesPerRegion())
                        );

        String hfilePath =
                jobOption.getOutHBaseHdfsPath() == null ?
                        AbstractImpJob.defaultOutHBaseHdfsPath(jobOption.getHbaseTablename()) : jobOption.getOutHBaseHdfsPath();
        hfileRdd.saveAsNewAPIHadoopFile(
                hfilePath,
                ImmutableBytesWritable.class, KeyValue.class,
                HFileOutputFormat2.class, confx);

        /**
         * bulkload 客户端需要加上如下环境变量：
         * export HADOOP_HOME=/xxx/xxx/xxx
         * export JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:$HADOOP_HOME/lib/native
         * export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native
         * export SPARK_YARN_USER_ENV="JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH,LD_LIBRARY_PATH=$LD_LIBRARY_PATH"
         */
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(hfilePath), connection.getAdmin(), connection.getTable(tn), connection.getRegionLocator(tn));
    }

    /**
     * job 详情
     * @param args
     * @return
     * @throws IOException
     */
    private HiveToHbaseJobOption buildJobOptionInfo(String... args) throws IOException {
        String path = ETL_JOBS_HDFS_ROOTDIR + args[0];
        HiveToHbaseJobOption jobOption = new HiveToHbaseJobOption();
        Properties properties = this.propsFromHdfs(path);
        jobOption.setSparkSql(this.parseSqlDynamicParams(properties.getProperty("query")));
        jobOption.setHbaseTablename(properties.getProperty("hbase_name"));
        jobOption.setHbaseColumnfamily(properties.getProperty("col_family"));
        jobOption.setNumberOfFilesPerRegion(Integer.parseInt(properties.getProperty("hfile_nums", "1").trim()));
        jobOption.setOutHBaseHdfsPath(AbstractImpJob.defaultOutHBaseHdfsPath(jobOption.getHbaseTablename()));
        Map<String, Object> rkColumnsMap =
                JSONObject.parseObject(properties.getProperty("row_key"), new HashMap<String, Object>().getClass());
        List<? extends HashMap> rkColumnsList =
                JSONObject.parseArray(
                        JSONObject.toJSONString(rkColumnsMap.get("row_key")),
                        new HashMap<String, Object>().getClass()
                );
        List<RkColumn> rkColumns = new ArrayList<>();
        for(Map kv : rkColumnsList) {
            RkColumn rkColumn = new RkColumn();
            rkColumn.setName((kv.get("col_name") + ""));
            rkColumn.hashkeyOrNot(Integer.parseInt(kv.get("is_hash") + ""));
            rkColumn.setPriority(Integer.parseInt(kv.get("prio") + ""));
            rkColumns.add(rkColumn);
        }
        rkColumns.sort(new Comparator<RkColumn>() {
            @Override
            public int compare(RkColumn o1, RkColumn o2) {
                if(o1.getPriority() == o2.getPriority())
                    return o1.getName().compareTo(o2.getName());
                return o1.getPriority() > o2.getPriority() ? 1 : -1;
            }
        });
        jobOption.setRkColumns(rkColumns);
        jobOption.setNumDriverCores(
                this.intParamValueLimit(Integer.parseInt(properties.getProperty("spark_driver_cores", "1")), 1, 4)
        );
        jobOption.setGbOfDriverMemory(
                this.intParamValueLimit(Integer.parseInt(properties.getProperty("spark_driver_memory", "2")), 1, 8)
        );
        jobOption.setNumExecutors(
                this.intParamValueLimit(Integer.parseInt(properties.getProperty("spark_num_executor", "2")), 2, 10)
        );
        jobOption.setNumExecutorCores(
                this.intParamValueLimit(Integer.parseInt(properties.getProperty("spark_executor_cores", "2")), 1, 4)
        );
        jobOption.setGbOfExecutorMemory(
                this.intParamValueLimit(Integer.parseInt(properties.getProperty("spark_executor_memory", "8")), 1, 16)
        );
        System.out.println("jobOption.info: " + JSONObject.toJSON(jobOption));
        return jobOption;
    }

    /**
     * sql 动参解析
     * @param sql
     * @param args
     * @return
     */
    private String parseSqlDynamicParams(String sql, String... args) {
        if(args == null || args.length < 2) return sql;
        Map<String, String> params = new HashMap<>();
        int len = args.length;
        for(int i = 1; i < len; i++) {
            String key = args[i].split("=")[0].trim();
            String value = args[i].split("=")[1].trim();
            params.put(key, value);
        }
        return ParseCommand.parseCommand(sql, params);
    }

    private int intParamValueLimit(Integer v, Integer min, Integer max) {
        return Math.max(min, Math.min(max, v));
    }

    private Properties propsFromHdfs(String path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.newInstance(conf);
        FSDataInputStream stream = fs.open(new Path(path));
        Properties prop = new Properties();
        //读取svn配置文件
        prop.load(new InputStreamReader(stream,"utf-8"));
        try{stream.close();} catch (Exception e) {}
        try{fs.close();} catch (Exception e) {}
        return prop;
    }

    public static void main(String... args) throws Exception {
        HiveToHbaseJob job = new HiveToHbaseJob();
        job.run(args);
    }
}
