package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.common.StatusEnum;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description zookeeper node 状态
 **/
public class ZkStateMetric extends BaseMetric {

    StatusEnum status = StatusEnum.UNKNOWN;

    // 连接数
    Integer connections;

    // 最大延迟
    Integer maxLatency;

    // 平均延迟
    Integer avgLatency;

    public ZkStateMetric(String ip, Integer port) {
        super(ip, port);
    }

    public StatusEnum getStatus() {
        return status;
    }

    public void setStatus(StatusEnum status) {
        this.status = status;
    }

    public Integer getConnections() {
        return connections;
    }

    public void setConnections(Integer connections) {
        this.connections = connections;
    }

    public Integer getMaxLatency() {
        return maxLatency;
    }

    public void setMaxLatency(Integer maxLatency) {
        this.maxLatency = maxLatency;
    }

    public Integer getAvgLatency() {
        return avgLatency;
    }

    public void setAvgLatency(Integer avgLatency) {
        this.avgLatency = avgLatency;
    }
}
