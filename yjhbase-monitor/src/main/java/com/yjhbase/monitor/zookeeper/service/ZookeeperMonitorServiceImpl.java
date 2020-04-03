package com.yjhbase.monitor.zookeeper.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.common.StatusEnum;
import com.yjhbase.monitor.metrics.BaseMetric;
import com.yjhbase.monitor.metrics.ZkStateMetric;
import com.yjhbase.monitor.prometheus.MetricFamilySamples;
import com.yjhbase.monitor.prometheus.Sample;
import com.yjhbase.monitor.prometheus.TextFormat;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@Service
public class ZookeeperMonitorServiceImpl implements ZookeeperMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> zkNodes = null;

    @PostConstruct
    public void prepare() {
        this.zkNodes = this.serverConfig.getZkNodes();
    }

    @Override
    public synchronized void buildZookeepersMonitorInfoWithPrometheusFormat(Writer writer) throws Exception{
        List<ZkStateMetric> zkListMetrics = this.getZookeepersMonitorInfo();
        List<MetricFamilySamples> samplesList = new ArrayList<>();
        samplesList.add(statusMetric(zkListMetrics));
        samplesList.add(connectionsMetric(zkListMetrics));
        TextFormat.write004(writer, samplesList);
    }

    /**
     * 状态信息
     * @param zkListMetrics
     * @return
     */
    private MetricFamilySamples statusMetric(List<ZkStateMetric> zkListMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(ZOOKEEPER_STATUS.getKey(), ZOOKEEPER_STATUS.getValue());
        for(ZkStateMetric zkMetrics : zkListMetrics) {
            Sample sample = Sample.build(familySamples.name, serverConfig.getClusterId());
            sample.addLable("nodeId", zkMetrics.getIp());
            sample.setSampleValue(zkMetrics.getStatus().getStatusId());
            familySamples.addSample(sample);
        }
        return familySamples;
    }

    /**
     * 连接数信息
     * @param zkListMetrics
     * @return
     */
    private MetricFamilySamples connectionsMetric(List<ZkStateMetric> zkListMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(ZOOKEEPER_CONNECTIONS.getKey(), ZOOKEEPER_CONNECTIONS.getValue());
        for(ZkStateMetric zkMetrics : zkListMetrics) {
            if(!zkMetrics.getStatus().equalTo(StatusEnum.ONLINE)) continue;
            Sample sample = Sample.build(familySamples.name, serverConfig.getClusterId());
            sample.addLable("nodeId", zkMetrics.getIp());
            sample.setSampleValue(zkMetrics.getConnections());
            familySamples.addSample(sample);
        }
        return familySamples;
    }

    /**
     * zookeeper 监控信息
     * @return
     */
    private List<ZkStateMetric> getZookeepersMonitorInfo() {
        List<ZkStateMetric> zkListMetrics = new ArrayList<>();
        for(ServerConfig.ServerNode node : this.zkNodes) {
            zkListMetrics.add(ZkStateMetric.parse(node.getIp(), node.getPort()));
        }
        return zkListMetrics;
    }

}
