package com.yjhbase.etl.jobs.imp;

import io.leopard.javahost.JavaHost;
import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description
 **/
public abstract class AbstractImpJob implements Serializable {

    static String PARAM_YJHBASE_REGION_HFILES_NUMBER = "yjhbase.region.hfiles.number";

    static String hbaseZookeeper = "10.0.43.94:2181,10.0.43.187:2181,10.0.43.65:2181";
    static String hbaseZnode = "/yjhbase";

    public AbstractImpJob(){
    }

    public abstract  void run() throws Exception;

    static String defaultOutHBaseHdfsPath(String hbaseTablename) {
        return "hdfs://hbasedfs/etl/imp/" +
                hbaseTablename.replace(":", "_ns_") + "_" + System.currentTimeMillis();
    }

    //hdfs 配置
    static Configuration hdfsConfiguration(Configuration confx) {
        confx.set("fs.defaultFS", "hdfs://nameservice1");
        confx.set("dfs.nameservices", "hbasedfs,nameservice1");
        confx.set("dfs.ha.namenodes.nameservice1", "nn1,nn2");
        confx.set("dfs.namenode.rpc-address.nameservice1.nn1", "TXIDC63-bigdata-hadoop-namenode1:8020");
        confx.set("dfs.namenode.rpc-address.nameservice1.nn2", "TXIDC64-bigdata-hadoop-namenode2:8020");
        confx.set("dfs.client.failover.proxy.provider.nameservice1",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        confx.set("dfs.ha.namenodes.hbasedfs", "nn1,nn2");
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.43.70:9000");
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.43.78:9000");
        confx.set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return confx;
    }

    //hbase host & ip
    public static void jvmHost() {
        String[] kuduNodes = new String[] {
                "10.0.43.94,txidc-bigdata-hbasekv1",
                "10.0.43.187,txidc-bigdata-hbasekv2",
                "10.0.43.65,txidc-bigdata-hbasekv3",
                "10.0.43.70,txidc-bigdata-hbasekv4",
                "10.0.43.78,txidc-bigdata-hbasekv5"
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
