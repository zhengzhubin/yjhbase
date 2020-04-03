package com.yjhbase.monitor.zookeeper.service;

import com.yjhbase.monitor.dto.Pair;
import com.yjhbase.monitor.metrics.ZkStateMetric;

import java.io.Writer;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public interface ZookeeperMonitorService {

    // Pair<$metric.name, $metric.description>
    static Pair<String, String> ZOOKEEPER_STATUS =
            new Pair<>("zookeeperStatus", "zookeeper status metric.");

    static Pair<String, String> ZOOKEEPER_CONNECTIONS =
            new Pair<>("zookeeperConnections", "zookeeper connections metric.");

    void buildZookeepersMonitorInfoWithPrometheusFormat(Writer writer) throws Exception;

}
