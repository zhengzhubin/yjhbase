package com.yjhbase.tools.utils;

import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
        if(connection == null || connection.isClosed() || connection.isAborted()) {
            connection = ConnectionFactory.createConnection(defaultConfiguration());
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
        conf.set("hbase.zookeeper.quorum" , "10.0.113.217:2181,10.0.114.255:2181,10.0.112.202:2181");
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

    //hbase host & ip
    public static void jvmHost() {
        String[] nodes = new String[] {
                "10.0.112.140,TXIDC-kudumaidian-cluster5",
                "10.0.113.196,TXIDC-kudumaidian-cluster4",
                "10.0.112.202,TXIDC-kudumaidian-cluster3",
                "10.0.114.255,TXIDC-kudumaidian-cluster2",
                "10.0.113.217,txidc-kudumaidian-cluster1"
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
