package com.yjhbase.monitor.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@Component
public class ServerConfig {

    static final Logger LOG = LoggerFactory.getLogger(ServerConfig.class);

    @Value("${yjhbase.cluster.id}")
    String clusterId;

    //dataformat: node1,node2,...,nodeN/port
    @Value("${yjhbase.zookeeper.nodes}")
    String zkNodes;

    @Value("${yjhbase.hdfs.namenodes}")
    String hdfsNamenodes;

    @Value("${yjhbase.hbase.masters}")
    String hbaseMasters;

    @Value("${yjhbase.hbase.regionservers}")
    String hbaseRegionservers;

    @Value("${yjhbase.hbase.region.files.limit}")
    Integer numberOfRegionHfilesLimit;

    @Value("${yjhbase.hbase.region.sizeGB.limit}")
    Integer gbOfRegionSizeLimit;

    public String getClusterId() {
        return this.clusterId;
    }

    /**
     * zookeeper 节点
     * @return
     */
    public List<ServerNode> getZkNodes(){
        return this.parse(this.zkNodes);
    }

    /**
     *  hdfs namenodes
     * @return
     */
    public List<ServerNode> getHdfsnamenodes(){
        return this.parse(this.hdfsNamenodes);
    }

    /**
     * hbase masters
     * @return
     */
    public List<ServerNode> getHBaseMasters() {
        return this.parse(this.hbaseMasters);
    }

    /**
     * hbase regionservers
     * @return
     */
    public List<ServerNode> getHBaseRegionservers() {
        return this.parse(this.hbaseRegionservers);
    }



    private List<ServerNode> parse(String nodesStr) {
        String[] ips = nodesStr.split("/")[0].split(",");
        Integer port = Integer.parseInt(nodesStr.split("/")[1].trim());

        List<ServerNode> retNodes = new ArrayList<>();
        for(String ip : ips) {
            retNodes.add(new ServerNode(ip, port));
        }
        return retNodes;
    }

    public static class ServerNode{

        String ip;
        Integer port;

        public ServerNode(String ip, Integer port){
            this.ip = ip;
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }
    }

}
