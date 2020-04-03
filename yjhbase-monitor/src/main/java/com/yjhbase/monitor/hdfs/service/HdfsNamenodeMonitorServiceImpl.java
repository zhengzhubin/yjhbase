package com.yjhbase.monitor.hdfs.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.common.StatusEnum;
import com.yjhbase.monitor.metrics.*;
import com.yjhbase.monitor.prometheus.MetricFamilySamples;
import com.yjhbase.monitor.prometheus.Sample;
import com.yjhbase.monitor.prometheus.TextFormat;
import com.yjhbase.monitor.utils.OkHttpUtil;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@Service
public class HdfsNamenodeMonitorServiceImpl implements HdfsNamenodeMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsNamenodeMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> namenodes = null;

    Map<String, HdfsNamenodeMetric> nodesSnapshot = new HashMap<>();

    @PostConstruct
    public void prepare() {
        this.namenodes = this.serverConfig.getHdfsnamenodes();
    }


    @Override
    public synchronized void buildHdfsnamenodeMonitorInfoWithPrometheusFormat(Writer writer) throws Exception {
        List<HdfsNamenodeMetric> nodesMetrics = this.getHdfsnamenodeMonitorInfo();

        List<MetricFamilySamples> samplesList = new ArrayList<>();
        samplesList.add(statusMetric(nodesMetrics));
        samplesList.add(jvmMetrics(nodesMetrics));
        samplesList.add(infoMetrics(nodesMetrics));
        TextFormat.write004(writer, samplesList);
        this.updateShopshot(nodesMetrics);
    }

    private MetricFamilySamples infoMetrics(List<HdfsNamenodeMetric> nodesMetrics){
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HDFS_INFO_METRICS.getKey(), HDFS_INFO_METRICS.getValue());

        for(HdfsNamenodeMetric nodeMetrics : nodesMetrics) {
            HdfsNamenodeInfoMetrics infoMetrics = nodeMetrics.getNamenodeInfoMetrics();
            if (infoMetrics == null) continue;

            Sample onSafeMode = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"onSafeMode", infoMetrics.getOnSafeMode() ? 1 : 0);
            Sample percentOfDiskRemaining = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"percentOfDiskRemaining", infoMetrics.getPercentOfDiskRemaining());
            Sample totalFiles = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"totalFiles", infoMetrics.getTotalFiles());
            Sample totalBlocks = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"totalBlocks", infoMetrics.getTotalBlocks());
            Sample numberOfMissingBlocks = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"numberOfMissingBlocks", infoMetrics.getNumberOfMissingBlocks());
            Sample numberOfMissingBlocksWithReplicationFactorOne = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"numberOfMissingBlocksWithReplicationFactorOne", infoMetrics.getNumberOfMissingBlocksWithReplicationFactorOne());
            Sample numberOfLiveDataNodes = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"numberOfLiveDataNodes", infoMetrics.getNumberOfLiveDataNodes());
            Sample numberOfDeadDatanodes = this.getSample(familySamples.name,
                    nodeMetrics.getIp(),"numberOfDeadDatanodes", infoMetrics.getNumberOfDeadDatanodes());
            familySamples.addSample(onSafeMode)
                .addSample(percentOfDiskRemaining)
                .addSample(totalFiles)
                .addSample(totalBlocks)
                .addSample(numberOfMissingBlocks)
                .addSample(numberOfMissingBlocksWithReplicationFactorOne)
                .addSample(numberOfLiveDataNodes)
                .addSample(numberOfDeadDatanodes);
        }
        return familySamples;
    }

    /**
     * jvm 相关监控信息
     * @param nodesMetrics
     * @return
     */
    private MetricFamilySamples jvmMetrics(List<HdfsNamenodeMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HDFS_JVM_METRICS.getKey(), HDFS_JVM_METRICS.getValue());
        for(HdfsNamenodeMetric nodeMetrics : nodesMetrics) {
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
    private MetricFamilySamples statusMetric(List<HdfsNamenodeMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HDFS_NN_STATUS.getKey(), HDFS_NN_STATUS.getValue());
        for(HdfsNamenodeMetric nodeMetrics : nodesMetrics) {
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

    private List<HdfsNamenodeMetric> getHdfsnamenodeMonitorInfo() {
        List<HdfsNamenodeMetric> nodesMetrics = new ArrayList<>();
        for(ServerConfig.ServerNode node : this.namenodes) {
            nodesMetrics.add(HdfsNamenodeMetric.parse(node.getIp(), node.getPort()));
        }
        return nodesMetrics;
    }

    private void updateShopshot(List<HdfsNamenodeMetric> nodesMetrics) {
        nodesSnapshot = new HashMap<>();
        for(HdfsNamenodeMetric node : nodesMetrics) {
            nodesSnapshot.put(node.getIp(), node);
        }
    }
}
