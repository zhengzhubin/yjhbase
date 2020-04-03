package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.common.StatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public class HBaseMasterMetric extends BaseMetric{
    private final static Logger LOG = LoggerFactory.getLogger(HBaseMasterMetric.class);

    StatusEnum status = StatusEnum.UNKNOWN;

    JvmMetrics jvmMetrics;

    private HBaseMasterMetric(String ip, Integer port) {
        super(ip, port);
    }

    public static HBaseMasterMetric parse(String ip, Integer port) {
        HBaseMasterMetric masterMetric = new HBaseMasterMetric(ip, port);
        masterMetric.parse();
        return masterMetric;
    }

    public HBaseMasterMetric parse() {
        try {
            String jvmMetricsJson = this.httpRequest(
                    this.getBaseRequestUrl() + "Hadoop:service=HBase,name=JvmMetrics");
            this.jvmMetrics = JvmMetrics.parseFromHBaseMaster(jvmMetricsJson);
        }catch (Exception e) {
            LOG.error("Get hbase master metrics faield, node.host = " + this.getIp() + ", node.port = "+ this.getPort(), e);
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
}
