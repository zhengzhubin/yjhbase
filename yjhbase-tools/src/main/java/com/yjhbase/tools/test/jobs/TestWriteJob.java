package com.yjhbase.tools.test.jobs;

import com.yjhbase.tools.utils.YjHBaseClientUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author zhengzhubin
 * @date 2020/4/9
 * @description
 **/
public class TestWriteJob {

    static final Logger LOG = LoggerFactory.getLogger(TestWriteJob.class);
    ThreadPoolExecutor pool;
    static long rowsPerThread = 100000000;

    static {
        System.setProperty("HADOOP_USER_NAME" , "hdfs");
    }

    void run(int numTasks) throws Exception {
        pool = new ThreadPoolExecutor(4, 10, 30,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        List<Future<Long>> resultList = new ArrayList<>();
        for(int i = 0; i < numTasks; i++){
            Future<Long> result = this.pool.submit(new WriteTask("t_testwrite", rowsPerThread));
            resultList.add(result);
        }
        while (true) {
            boolean finishFlag = true;
            for(Future<Long> result : resultList) {
                if(!result.isDone()) {
                    finishFlag = false;
                    break;
                }
            }
            if(finishFlag) break;
            Thread.sleep(10000);
        }
        System.exit(0);
    }

    public static void main(String... args) throws Exception {
        TestWriteJob job = new TestWriteJob();
        job.run(args == null || args.length == 0 ? 1 : Integer.parseInt(args[0]));
    }

    private class WriteTask implements Callable<Long> {
        String tablename;
        Long rows;
        private WriteTask(String tablename, Long rows){
            this.tablename = tablename;
            this.rows = rows;
        }

        HTable table = null;
        List<Pair<byte[], byte[]>> columns;
        private void prepare() throws IOException {
            Connection connection = YjHBaseClientUtil.getTestClusterClient();
            table = (HTable) connection.getTable(TableName.valueOf(tablename));
            columns = new ArrayList<>();
            for(int i = 0; i< 30; i++) {
                String column = String.format("col%03d", i);
                columns.add(new Pair<>(Bytes.toBytes("f"), Bytes.toBytes(column)));
            }
        }

        @Override
        public Long call() throws Exception {
            this.prepare();
            Long ts = System.currentTimeMillis();
            int cnt = 0;
            while (this.rows > 0) {
                this.rows --;
                this.table.put(this.buildPut());
                this.table.put(this.buildPuts());
                System.out.println("============+"+cnt+"+========");
                cnt ++;
            }
            this.table.close();
            System.out.println("took.millis => " + (System.currentTimeMillis() - ts));
            return System.currentTimeMillis() - ts;
        }

        private List<Put> buildPuts() {
            List<Put> puts = new ArrayList<>();
            for(int i = 0; i< 100; i++) {
                puts.add(this.buildPut());
            }
            return puts;
        }

        private Put buildPut() {
            Put put = new Put(Bytes.toBytes(this.randomRowkey(20)));
            for(Pair<byte[], byte[]> c : columns) {
                put.addColumn(c.getFirst(), c.getSecond(), Bytes.toBytes(this.randomString(20)));
            }
            return put;
        }

        /**
         * 随机主键
         * @param length
         * @return
         */
        private String randomRowkey(int length) {
            String rowkey = this.randomString(length);
            int code = (rowkey.hashCode() & Integer.MAX_VALUE) % 1000;
            return String.format("%03d%s%s", code, "::", rowkey);
        }

        Random random = new Random();
        private String randomString(int length){
            String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            StringBuffer sb=new StringBuffer();
            for(int i=0;i<length;i++){
                int number=random.nextInt(62);
                sb.append(str.charAt(number));
            }
            return sb.toString();
        }
    }
}
