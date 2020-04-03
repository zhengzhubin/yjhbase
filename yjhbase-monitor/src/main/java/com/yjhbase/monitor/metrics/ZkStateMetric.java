package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.common.StatusEnum;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description zookeeper node 状态
 **/
public class ZkStateMetric extends BaseMetric {

    private static final Logger LOG = LoggerFactory.getLogger(ZkStateMetric.class);

    StatusEnum status = StatusEnum.UNKNOWN;

    // 连接数
    Long connections = 0L;

    // 最大延迟
    Long maxLatency = 0L;

    // 平均延迟
    Long avgLatency = 0L;

    private ZkStateMetric(String ip, Integer port) {
        super(ip, port);
    }

    public static ZkStateMetric parse(String ip, Integer port) {
        ZkStateMetric zkStateMetric = new ZkStateMetric(ip, port);
        try {
            String statInfo = FourLetterWordMain.send4LetterWord(ip, port, "stat");
            if(statInfo == null || statInfo.length() == 0) {
                throw new RuntimeException("invalid result, stat info is null.");
            }
            String[] lines = statInfo.split("\n");
            for(String line: lines) {
                if(line.trim().startsWith("Connections: ")) {
                    zkStateMetric.connections = Long.parseLong(line.replace("Connections: " , "").trim());
                } else if(line.trim().startsWith("Latency ")) {
                    String[] latency = line.replace("Latency ","").split(":")[1].split("/");
                    zkStateMetric.avgLatency = Long.parseLong(latency[1].trim());
                    zkStateMetric.maxLatency = Long.parseLong(latency[2].trim());
                }
            }
            zkStateMetric.status = StatusEnum.ONLINE;
        }catch (Exception e) {
            LOG.error("Get zookeeper state metrics faield, node.host = " + ip + ", node.port = "+ port, e);
            zkStateMetric.status = StatusEnum.OFFLINE;
        }
        return zkStateMetric;
    }

    public StatusEnum getStatus() {
        return status;
    }

    public Long getConnections() {
        return connections;
    }

    public Long getMaxLatency() {
        return maxLatency;
    }

    public Long getAvgLatency() {
        return avgLatency;
    }
}
