package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.metrics.HBaseMasterMetric;
import com.yjhbase.monitor.metrics.JvmMetrics;
import com.yjhbase.monitor.prometheus.MetricFamilySamples;
import com.yjhbase.monitor.prometheus.Sample;
import com.yjhbase.monitor.prometheus.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
@Service
public class HBaseMasterMonitorServiceImpl implements HBaseMasterMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseMasterMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> masters = null;

    Map<String, HBaseMasterMetric> nodesSnapshot = new HashMap<>();

    @PostConstruct
    public void prepare() {
        this.masters = this.serverConfig.getHBaseMasters();
    }

    @Override
    public synchronized void buildHBaseMasterMonitorInfoWithPrometheusFormat(Writer writer) throws Exception {
        List<HBaseMasterMetric> mastersMetrics = this.getHBaseMasterMonitorInfo();

        List<MetricFamilySamples> samplesList = new ArrayList<>();
        samplesList.add(statusMetric(mastersMetrics));
        samplesList.add(jvmMetrics(mastersMetrics));

        TextFormat.write004(writer, samplesList);
        this.updateShopshot(mastersMetrics);
    }

    /**
     * jvm 相关监控信息
     * @param nodesMetrics
     * @return
     */
    private MetricFamilySamples jvmMetrics(List<HBaseMasterMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_MASTER_JVM_METRICS.getKey(), HBASE_MASTER_JVM_METRICS.getValue());
        for(HBaseMasterMetric nodeMetrics : nodesMetrics) {
            JvmMetrics jvmMetrics =  nodeMetrics.getJvmMetrics();
            if(jvmMetrics == null) continue;

            Sample memHeapUsedM = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"memHeapUsedM", jvmMetrics.getMemHeapUsedM());
            Sample runnableThreads = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"runnableThreads", jvmMetrics.getRunnableThreads());
            Sample blockedThreads = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"blockedThreads", jvmMetrics.getBlockedThreads());
            familySamples.addSample(memHeapUsedM)
                    .addSample(runnableThreads)
                    .addSample(blockedThreads);

            if(!this.nodesSnapshot.containsKey(nodeMetrics.getIp())) continue;
            JvmMetrics snapshotJvmMetrics =
                    this.nodesSnapshot.get(nodeMetrics.getIp()).getJvmMetrics();
            if(snapshotJvmMetrics == null) continue;
            Sample gcCount = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"gcCount",
                    intervalValue(jvmMetrics.getGcCount(), snapshotJvmMetrics.getGcCount())
            );
            Sample gcTimeMillis = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"gcTimeMillis",
                    intervalValue(jvmMetrics.getGcTimeMillis(), snapshotJvmMetrics.getGcTimeMillis())
            );
            familySamples.addSample(gcCount).addSample(gcTimeMillis);
        }
        return familySamples;
    }

    private long intervalValue(Long current, Long snapshot){
        return current < snapshot ? current : (current - snapshot);
    }

    /**
     * 节点状态
     * @param nodesMetrics
     * @return
     */
    private MetricFamilySamples statusMetric(List<HBaseMasterMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_MASTER_STATUS.getKey(), HBASE_MASTER_STATUS.getValue());
        for(HBaseMasterMetric nodeMetrics : nodesMetrics) {
            int status = nodeMetrics.getStatus().getStatusId();
            Sample sample = this.getSample(familySamples.name, nodeMetrics.getIp(), status);
            familySamples.addSample(sample);
        }
        return familySamples;
    }

    private Sample getSample(String familyname, String nodeId, double value) {
        return this.getSample(familyname, nodeId, null, value);
    }

    private Sample getSample(String familyname, String nodeId, String subLabel, double value) {
        Sample sample = Sample.build(familyname, serverConfig.getClusterId());
        sample.addLable("nodeId", nodeId);
        if(subLabel != null && subLabel.length() > 0) {
            sample.addLable("l2babel", subLabel);
        }
        sample.setSampleValue(value);
        return sample;
    }

    public List<HBaseMasterMetric> getHBaseMasterMonitorInfo() {
        List<HBaseMasterMetric> mastersMetrics = new ArrayList<>();
        for(ServerConfig.ServerNode master : this.masters) {
            mastersMetrics.add(HBaseMasterMetric.parse(master.getIp(), master.getPort()));
        }
        return mastersMetrics;
    }

    private void updateShopshot(List<HBaseMasterMetric> nodesMetrics) {
        nodesSnapshot = new HashMap<>();
        for(HBaseMasterMetric node : nodesMetrics) {
            nodesSnapshot.put(node.getIp(), node);
        }
    }
}
