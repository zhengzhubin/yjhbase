package com.yjhbase.tools.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class Scratch {
    public static void main(String[] args) {
        YjHBaseClientUtil.jvmHost();
        Configuration conf;
        Connection conn = null;
        final ExecutorService hbaseExecutorService =
                new ThreadPoolExecutor(2, 2,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(10));
        String zk = "212.129.139.188";
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zk);
        conf.set("zookeeper.znode.parent", "/hbase");
        conf.setInt("hbase.rpc.timeout", 60000);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        String tableName = "algorithm:t_rec_tab_u2i_recall_new_item";
        String rowKey = "980::105::28424941";
        TableBuilder tableBuilder =
                conn.getTableBuilder(TableName.valueOf(tableName), hbaseExecutorService);
        Integer opTimeout = 5000;
        tableBuilder.setOperationTimeout(opTimeout);
        tableBuilder.setRpcTimeout(opTimeout);
        tableBuilder.setReadRpcTimeout(opTimeout);

        long currentTS = EnvironmentEdgeManager.currentTime();

        for(int i = 0; i< 1001; i++) {
            Table table = tableBuilder.build();
            Get get = new Get(Bytes.toBytes(rowKey));
            try {
                long ts = System.currentTimeMillis();
                Result result = table.get(get);
                System.out.println("result is null ? " + (result == null));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            conn.close();
        } catch (IOException e) {
            conn = null;
        } finally {
            conn = null;
        }
        System.out.println("================== 累计耗时(ms)：" + (EnvironmentEdgeManager.currentTime() - currentTS) + " =============================");
    }
}
