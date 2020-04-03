package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.common.StatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public class HdfsNamenodeMetric extends BaseMetric{
    private final static Logger LOG = LoggerFactory.getLogger(HdfsNamenodeMetric.class);

    StatusEnum status = StatusEnum.UNKNOWN;

    JvmMetrics jvmMetrics;

    HdfsNamenodeInfoMetrics namenodeInfoMetrics;

    private HdfsNamenodeMetric(String ip, Integer port) {
        super(ip, port);
    }

    public static HdfsNamenodeMetric parse(String ip, Integer port){
        HdfsNamenodeMetric namenodeMetric = new HdfsNamenodeMetric(ip, port);
        return namenodeMetric.parse();
    }

    private HdfsNamenodeMetric parse() {
        try {
            String jvmInfoJson = this.httpRequest(
                    this.getBaseRequestUrl() + "Hadoop:service=NameNode,name=JvmMetrics");
            this.jvmMetrics = JvmMetrics.parseFromHdfsNamenode(jvmInfoJson);
            String nodeInfoJson = this.httpRequest(
                    this.getBaseRequestUrl() + "Hadoop:service=NameNode,name=NameNodeInfo");
            this.namenodeInfoMetrics = HdfsNamenodeInfoMetrics.parse(nodeInfoJson);
        } catch (Exception e) {
            LOG.error("Get hdfs namenode metrics faield, node.host = " + this.getIp() + ", node.port = "+ this.getPort(), e);
        }
        this.status = this.jvmMetrics == null ? StatusEnum.OFFLINE : StatusEnum.ONLINE;
        return this;
    }

    private String getBaseRequestUrl() {
        return "http://" + this.getIp() + ":" + this.getPort() + "/jmx?qry=";
    }

    public StatusEnum getStatus() {
        return status;
    }

    public JvmMetrics getJvmMetrics() {
        return jvmMetrics;
    }

    public HdfsNamenodeInfoMetrics getNamenodeInfoMetrics() {
        return namenodeInfoMetrics;
    }
}
