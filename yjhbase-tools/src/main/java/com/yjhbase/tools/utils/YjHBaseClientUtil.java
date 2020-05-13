package com.yjhbase.tools.utils;

import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description
 **/
public class YjHBaseClientUtil {

    private static Logger LOG = LoggerFactory.getLogger(YjHBaseClientUtil.class);

    static Connection connection = null;

    static {
        jvmHost();
    }

    public static synchronized Connection getClient() throws IOException {
        return getClient(new HashMap<>());
    }

    public static synchronized Connection getTestClusterClient() throws IOException {
        Map<String, String> params =  new HashMap<>();
        params.put("hbase.zookeeper.quorum", "212.129.139.188");
        params.put("zookeeper.znode.parent", "/hbase");
        return getClient(params);
    }

    public static synchronized Connection getClient(Map<String, String> params) throws IOException {
        if(connection == null || connection.isClosed() || connection.isAborted()) {
            Configuration conf = defaultConfiguration();
            for(Map.Entry<String, String> kv : params.entrySet()) {
                conf.set(kv.getKey(), kv.getValue());
            }
            connection = ConnectionFactory.createConnection(conf);
        }
        return connection;
    }

    public static HTable getTable(String table) {
        return getTable("default", table);
    }

    public static HTable getTable(String namespace, String table) {
        try {
            return (HTable) getClient().getTable(TableName.valueOf(namespace, table));
        } catch (IOException e) {
            throw new RuntimeException("get hbase table[ns = " + namespace + ", table = " + table + "] failed.", e);
        }
    }

    private static Configuration defaultConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum" , "10.0.43.94:2181,10.0.43.187:2181,10.0.43.65:2181");
        conf.set("zookeeper.znode.parent" , "/yjhbase");
        conf.setInt("zookeeper.session.timeout", 60000);
        conf.setInt("hbase.client.retries.number", 10);
        conf.setInt("hbase.client.pause", 100);
        conf.setInt("hbase.client.max.total.tasks", 1000);
        conf.setInt("hbase.client.max.perserver.tasks", 200);
        conf.setInt("hbase.client.max.perregion.tasks", 30);
        conf.setInt("hbase.rpc.timeout", 60000);
        return conf;
    }

    public static void main(String... args) throws Exception{
        Connection c = getTestClusterClient();
        Table t = c.getTable(TableName.valueOf("member"));
        Result rowResult = t.get(new Get(Bytes.toBytes("Sariel")));

        System.out.println(Bytes.toString(rowResult.getRow()));

    }

    public static void mainx(String... args) throws IOException {
        Connection c = getTestClusterClient();
        boolean flag = c.getAdmin().tableExists(TableName.valueOf("t_hbasetest"));
        HTable t  = (HTable) c.getTable(TableName.valueOf("t_hbasetest"));
        for(int i = 0; i< 10; i++) {
            Put put = new Put(Bytes.toBytes(String.format("%s%08d", "row_", i)));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("col1"), Bytes.toBytes(String.format("value___%08d", i)));
            t.put(put);
        }
        t.close();

        System.out.println(flag);
    }


    //hbase host & ip
    public static void jvmHost() {
        String[] nodes = new String[] {
                "10.0.43.94,txidc-bigdata-hbasekv1",
                "10.0.43.187,txidc-bigdata-hbasekv2",
                "10.0.43.65,txidc-bigdata-hbasekv3",
                "10.0.43.70,txidc-bigdata-hbasekv4",
                "10.0.43.78,txidc-bigdata-hbasekv5",
                "212.129.139.188,VM_4_55_centos"
        };
        Properties virtualDns = new Properties();
        for(String node: nodes) {
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
