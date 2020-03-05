package com.yjhbase.monitor.zookeeper.service;

import com.yjhbase.monitor.metrics.ZkStateMetric;

import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public interface ZookeeperMonitorService {

    /**
     * 获取zookeeper 信息
     * @return 节点状态、连接数、延迟信息
     */
    List<ZkStateMetric> getZkNodesStateMetric();

}
