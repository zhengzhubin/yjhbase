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

    static String hbaseZookeeper = "10.0.113.217:2181,10.0.114.255:2181,10.0.112.202:2181";
    static String hbaseZnode = "/yjhbase";

    public AbstractImpJob(){
    }

    public abstract  void run(ImpJobOption jobOption) throws Exception;

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
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn1", "10.0.113.196:9000");
        confx.set("dfs.namenode.rpc-address.hbasedfs.nn2", "10.0.112.140:9000");
        confx.set("dfs.client.failover.proxy.provider.hbasedfs",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        return confx;
    }

    //hbase host & ip
    static void jvmHost() {
        String[] kuduNodes = new String[] {
                "10.0.112.140,TXIDC-kudumaidian-cluster5",
                "10.0.113.196,TXIDC-kudumaidian-cluster4",
                "10.0.112.202,TXIDC-kudumaidian-cluster3",
                "10.0.114.255,TXIDC-kudumaidian-cluster2",
                "10.0.113.217,txidc-kudumaidian-cluster1",
                "10.0.40.220,TXIDC417-bigdata-kudu-10",
                "10.0.40.167,TXIDC416-bigdata-kudu-9",
                "10.0.40.207,TXIDC415-bigdata-kudu-8",
                "10.0.40.189,TXIDC414-bigdata-kudu-7",
                "10.0.40.236,TXIDC413-bigdata-kudu-6",
                "10.0.40.248,TXIDC412-bigdata-kudu-5",
                "10.0.40.234,TXIDC411-bigdata-kudu-4",
                "10.0.40.215,TXIDC410-bigdata-kudu-3",
                "10.0.40.158,TXIDC409-bigdata-kudu-2",
                "10.0.40.240,TXIDC408-bigdata-kudu-1"
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
