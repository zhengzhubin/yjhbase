package com.yjhbase.tools.test.jobs.consumers;

import com.yjhbase.tools.utils.YjHBaseClientUtil;
//import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description
 **/
public class ConsumersGetJob {

    static final Logger LOG = LoggerFactory.getLogger(ConsumersGetJob.class);

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
    }

    ThreadPoolExecutor pool;
//    Connection connection = null;
    HTable table = null;

    public void prepare() throws IOException {
        pool = new ThreadPoolExecutor(10, 10,
                30, TimeUnit.SECONDS, new LinkedBlockingDeque());
        this.table = YjHBaseClientUtil.getTable("t_consumer_items");
    }

    public void run(Set<Integer> ids) throws IOException {
        List<ConsumerDto> consumers = this.listConsumers(ids);
        for(ConsumerDto consumer : consumers) {
            GetTask task  = new GetTask(this.table, consumer.consumerId);
            this.pool.execute(task);
        }

        LOG.info("all tasks submit success.");
    }

    public static void main(String... args) throws IOException {
        Set<Integer> ids = new HashSet<>();
        String[] list = args[0].split(",");
        for(String id : list) {
            ids.add(Integer.parseInt(id));
        }

        ConsumersGetJob job = new ConsumersGetJob();
        job.prepare();
        job.run(ids);

    }

    List<ConsumerDto> listConsumers(Set<Integer> ids) throws IOException {
        List<ConsumerDto> retConsumers = new ArrayList<>();
        Configuration conf = this.hdfsConfiguration();
        FileSystem fs = FileSystem.get(conf);
        for(int i = 0; i < 100; i ++) {
            if(!ids.contains(i % 10)) continue;
            String file = String.format("part-%05d", i);
            retConsumers.addAll(listConsumers(fs, file));
        }
        LOG.info("retConsumers.length = " + retConsumers.size());
        try{
            LOG.info("close hdfs fs.");
            fs.close();
        }catch (Exception e) { }

        retConsumers.sort(new Comparator<ConsumerDto>() {
            @Override
            public int compare(ConsumerDto o1, ConsumerDto o2) {
                if(o1.score.intValue() == o2.score.intValue()) {
                    return o1.consumerId.compareTo(o2.consumerId);
                }
                return o1.score.intValue() > o2.score.intValue() ? 1 : -1;
            }
        });
        return retConsumers;
    }

    private List<ConsumerDto> listConsumers(FileSystem fs, String file) throws IOException {
        LOG.info("open file: " + "/etl/others/t_consumers/" +file);
        FSDataInputStream ins = fs.open(new Path("/etl/others/t_consumers/" +file));
        List<ConsumerDto> consuemrsId = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins, "UTF-8"));
        String line = null;
        while ((line = reader.readLine()) != null) {
            if(StringUtils.isBlank(line)) continue;
            consuemrsId.add(new ConsumerDto(line, score()));
        }
        try{
            ins.close();
            reader.close();
        }catch (Exception e) { }
        LOG.info("close file: " + "/etl/others/t_consumers/" +file + ", len = " + consuemrsId.size());
        return consuemrsId;
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

    public class ConsumerDto {
        String consumerId;

        Integer score;

        public ConsumerDto(String consumerId, Integer score) {
            this.consumerId = consumerId;
            this.score = score;
        }
    }

    public class GetTask implements Runnable {
        public HTable table = null;
        String consumerId = null;
        public GetTask(HTable table, String consumerId) {
            this.table = table;
            this.consumerId = consumerId;
        }

        @Override
        public void run() {
            SummaryTool.requests(this.consumerId);
            String rkString = this.converseConsumerId(0); //0:不分页，即全部数据
            // 不分页查询
            Get getRequest = new Get(Bytes.toBytes(rkString));
            try {
                Long beforeMS = System.currentTimeMillis();
                Result resp = this.table.get(getRequest);
                long spentTS = System.currentTimeMillis() - beforeMS;
                //数据统计
                SummaryTool.success(spentTS, rkString);
            } catch (IOException e) {
                SummaryTool.failed(e);
            }
        }

        private String converseConsumerId(int pageId) {
            int code = (this.consumerId.hashCode() & Integer.MAX_VALUE) % 1000;
            return String.format("%03d::%s::%02d", code, this.consumerId, pageId);
        }
    }

}
