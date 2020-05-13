package com.yjhbase.tools.test.jobs.consumers;

import com.yjhbase.tools.utils.YjHBaseClientUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description
 **/
public class ConsumersGetV2Job {

    static final Logger LOG = LoggerFactory.getLogger(ConsumersGetV2Job.class);

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
    }
    Connection connection = null;
    ThreadPoolExecutor pool;
    static long keepAliveTime = 10L;
    static ExecutorService hbaseExecutorService = null;


    public synchronized void prepare(int numThreads) throws IOException {
        pool =
                new ThreadPoolExecutor(numThreads, numThreads,
                        keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingDeque());
        this.connection = YjHBaseClientUtil.getClient();
        if(hbaseExecutorService != null) return;
        hbaseExecutorService =
                new ThreadPoolExecutor(numThreads, numThreads,
                        keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
    }

    public void run() throws IOException {
        Set<Integer> parts = new HashSet<>();
        for(int part = 0; part < 20; part++) parts.add(part);
        List<TaskDto> tasksDto = this.listConsumers(parts);
        for(TaskDto taskDto : tasksDto) {
            GetTask task  = new GetTask(this.connection, taskDto.key);
            this.pool.execute(task);
        }

        LOG.info("all tasks submit success.");
    }

    public static void main(String... args) throws IOException {
        ConsumersGetV2Job job = new ConsumersGetV2Job();
        job.prepare(Math.min(Integer.parseInt(args[0]), 10));
        job.run();

    }

    List<TaskDto> listConsumers(Set<Integer> parts) throws IOException {
        List<TaskDto> tasksDto = new ArrayList<>();
        Configuration conf = this.hdfsConfiguration();
        FileSystem fs = FileSystem.get(conf);
        for(int partId : parts) {
            String file = String.format("part-%05d", partId);
            tasksDto.addAll(listConsumers(fs, file));
        }

        LOG.info("retTasksDto.length = " + tasksDto.size());
        try{
            LOG.info("close hdfs fs.");
            fs.close();
        }catch (Exception e) { }

        tasksDto.sort(new Comparator<TaskDto>() {
            @Override
            public int compare(TaskDto o1, TaskDto o2) {
                if(o1.score.intValue() == o2.score.intValue()) {
                    return o1.key.compareTo(o2.key);
                }
                return o1.score.intValue() > o2.score.intValue() ? 1 : -1;
            }
        });
        return tasksDto;
    }

    private List<TaskDto> listConsumers(FileSystem fs, String file) throws IOException {
        LOG.info("open file: " + "/etl/others/t_consumers/" +file);
        FSDataInputStream ins = fs.open(new Path("/etl/others/t_consumers/" +file));
        List<TaskDto> tasksDto = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins, "UTF-8"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if(StringUtils.isBlank(line)) continue;
            tasksDto.add(new TaskDto(line, score()));
        }
        try{
            ins.close();
            reader.close();
        }catch (Exception e) { }
        LOG.info("close file: " + "/etl/others/t_consumers/" +file + ", len = " + tasksDto.size());
        return tasksDto;
    }

    static Random random = new Random();
    static Integer score(){
        return random.nextInt(10000000);
    }

    private  Configuration hdfsConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", "hdfs://hbasedfs");
        conf.set("dfs.nameservices", "hbasedfs");
        conf.set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.43.70:9000");
        conf.set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.43.78:9000");
        conf.set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return conf;
    }

    public class TaskDto {
        String key;
        Integer score;

        public TaskDto(String key, Integer score) {
            this.key = key;
            this.score = score;
        }
    }

    public class GetTask implements Runnable {
        public Connection connection = null;
        String key = null;
        TableName tn = null;

        public GetTask(Connection connection, String key) {
            this.connection = connection;
            this.key = key;
            tn = TableName.valueOf("algorithm:t_rec_tab_u2i_recall_new_item");
        }

        @Override
        public void run() {
            SummaryTool.requests(this.key);
            Get getRequest = new Get(Bytes.toBytes(this.key));
            try {
                Long beforeMS = System.currentTimeMillis();
                HTable table = this.getHTable();
                table.get(getRequest);
                long spentTS = System.currentTimeMillis() - beforeMS;
                //数据统计
                SummaryTool.success(spentTS, this.key);
            } catch (IOException e) {
                SummaryTool.failed(e);
            }
        }

        int opTimeout = 20;
        int opRpcTimeout = 20;

        private HTable getHTable() {
            TableBuilder tableBuilder =
                    this.connection.getTableBuilder(this.tn, hbaseExecutorService);
            tableBuilder.setOperationTimeout(opTimeout);
            tableBuilder.setRpcTimeout(opRpcTimeout);
            tableBuilder.setReadRpcTimeout(opRpcTimeout);
//            tableBuilder.setWriteRpcTimeout(opRpcTimeout);
            return (HTable) tableBuilder.build();
        }
    }

}
