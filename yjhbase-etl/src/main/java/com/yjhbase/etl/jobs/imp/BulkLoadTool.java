package com.yjhbase.etl.jobs.imp;

import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zhengzhubin
 * @date 2020/2/26
 * @description
 **/
public class BulkLoadTool {

    public static void main(String...  args) throws IOException {
        jvmHost();
        Configuration conf = HBaseConfiguration.create();
        String zookeeper = "10.0.113.217:2181,10.0.114.255:2181,10.0.112.202:2181";
        conf.set("hbase.zookeeper.quorum" , zookeeper);
        conf.set("zookeeper.znode.parent" , "/yjhbase");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "t_items");
        TableName tn = TableName.valueOf("t_items");
        Connection connection = ConnectionFactory.createConnection(conf);

        conf.set("fs.defaultFS", "hdfs://nameservice1");
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
        String hfilePath = "hdfs://hbasedfs/etl/imp/t_items";
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path(hfilePath), connection.getAdmin(), connection.getTable(tn), connection.getRegionLocator(tn));
    }

    private static void jvmHost() {
        String[] kuduNodes = new String[] {
                "10.0.112.140,TXIDC-kudumaidian-cluster5",
                "10.0.113.196,TXIDC-kudumaidian-cluster4",
                "10.0.112.202,TXIDC-kudumaidian-cluster3",
                "10.0.114.255,TXIDC-kudumaidian-cluster2",
                "10.0.113.217,txidc-kudumaidian-cluster1"
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
