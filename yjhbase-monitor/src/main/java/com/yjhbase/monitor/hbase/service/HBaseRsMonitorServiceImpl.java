package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.common.StatusEnum;
import com.yjhbase.monitor.metrics.*;
import com.yjhbase.monitor.prometheus.MetricFamilySamples;
import com.yjhbase.monitor.prometheus.Sample;
import com.yjhbase.monitor.prometheus.TextFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.Writer;
import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
@Service
public class HBaseRsMonitorServiceImpl implements HBaseRsMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRsMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> regionservers = null;

    Map<String, HBaseRsMetric> nodesSnapshot = new HashMap<>();

    @PostConstruct
    public void prepare() {
        this.regionservers = this.serverConfig.getHBaseRegionservers();
    }

    @Override
    public synchronized void buildHBaseRsMonitorInfoWithPrometheusFormat(Writer writer) throws Exception {
        List<HBaseRsMetric> rsListMetrics = this.getHBaseRsMonitorInfo();

        List<MetricFamilySamples> samplesList = new ArrayList<>();
        samplesList.add(statusMetric(rsListMetrics));
        samplesList.add(jvmMetrics(rsListMetrics));
        samplesList.add(ipcMetrics(rsListMetrics));

        TextFormat.write004(writer, samplesList);
        this.updateShopshot(rsListMetrics);
    }

    private MetricFamilySamples tablesMetrics(List<HBaseRsMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_RS_TABLES_METRICS.getKey(), HBASE_RS_TABLES_METRICS.getValue());

        Map<String, TableMetrics> tables = this.getTablesMetrics(nodesMetrics);
        Map<String, TableMetrics> snapshotTables =
                this.getTablesMetrics(nodesSnapshot == null ? null : nodesSnapshot.values());

        for(TableMetrics t : tables.values()) {
            if(t instanceof TableMergedMetrics) {
                TableMergedMetrics merged = (TableMergedMetrics) t;
                Sample regionCount = this.getTableSample(familySamples.name, merged.tablename,
                                "regionCount", merged.regionCount);
                Sample storeFileCount = this.getTableSample(familySamples.name, merged.tablename,
                        "storeFileCount", merged.storeFileCount);
                Sample blockedRegionCount = this.getTableSample(familySamples.name, merged.tablename,
                        "blockedRegionCount", merged.blockedRegionCount);
                Sample storeFileSizeGB = this.getTableSample(familySamples.name, merged.tablename,
                        "storeFileSizeGB", merged.getStoreFileSizeGB());
                Sample tooBigRegionCount = this.getTableSample(familySamples.name, merged.tablename,
                        "tooBigRegionCount", merged.tooBigRegionCount);
                familySamples.addSample(regionCount)
                        .addSample(storeFileCount)
                        .addSample(blockedRegionCount)
                        .addSample(storeFileSizeGB)
                        .addSample(tooBigRegionCount);
            } else if(t instanceof TableUnMergedMetrics) {
                if(!snapshotTables.containsKey(t.tablename)) continue;
                TableUnMergedMetrics unMerged = (TableUnMergedMetrics) t;
                TableUnMergedMetrics snapshot = (TableUnMergedMetrics) snapshotTables.get(t.tablename);
                Sample scanOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "scanOps", intervalValue(unMerged.scanOps, snapshot.scanOps)
                        );
                Sample incrementOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                        "incrementOps", intervalValue(unMerged.incrementOps, snapshot.incrementOps)
                        );
                Sample deleteOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "deleteOps", intervalValue(unMerged.deleteOps, snapshot.deleteOps)
                        );
                Sample putOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "putOps", intervalValue(unMerged.putOps, snapshot.putOps)
                        );
                Sample batchDeleteOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "batchDeleteOps", intervalValue(unMerged.batchDeleteOps, snapshot.batchDeleteOps)
                        );
                Sample batchPutOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "batchPutOps", intervalValue(unMerged.batchPutOps, snapshot.batchPutOps)
                        );
                Sample appendOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "appendOps", intervalValue(unMerged.appendOps, snapshot.appendOps)
                        );
                Sample compactionOps =
                        this.getTableSample(familySamples.name, unMerged.tablename,
                                "compactionOps", intervalValue(unMerged.compactionOps, snapshot.compactionOps)
                        );
                familySamples.addSample(scanOps)
                        .addSample(appendOps)
                        .addSample(putOps)
                        .addSample(deleteOps)
                        .addSample(incrementOps)
                        .addSample(batchDeleteOps)
                        .addSample(batchPutOps)
                        .addSample(compactionOps);
                for(Map.Entry<String, Long> kv : unMerged.getsOfProcesstime.entrySet()) {
                    Sample getsOfProcesstime =
                            this.getTableSample(familySamples.name, unMerged.tablename,
                                kv.getKey(),
                                intervalValue(kv.getValue(), snapshot.getsOfProcesstime.get(kv.getKey()))
                            );
                    familySamples.addSample(getsOfProcesstime);
                }
            }
        }
        return familySamples;
    }

    /**
     * ipc 相关监控信息
     * @param nodesMetrics
     * @return
     */
    private MetricFamilySamples ipcMetrics(List<HBaseRsMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_RS_IPC_METRICS.getKey(), HBASE_RS_IPC_METRICS.getValue());
        for(HBaseRsMetric nodeMetrics : nodesMetrics) {
            HBaseRsIpcMetrics ipcMetrics = nodeMetrics.getIpcMetrics();
            if(ipcMetrics == null) continue;
            Sample numOpenConnections = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numOpenConnections", ipcMetrics.getNumOpenConnections());
            Sample numCallsInWriteQueue = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numCallsInWriteQueue", ipcMetrics.getNumOpenConnections());
            Sample numCallsInReadQueue = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numCallsInReadQueue", ipcMetrics.getNumOpenConnections());
            Sample numCallsInScanQueue = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numCallsInScanQueue", ipcMetrics.getNumOpenConnections());
            Sample numActiveWriteHandler = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numActiveWriteHandler", ipcMetrics.getNumOpenConnections());
            Sample numActiveReadHandler = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numActiveReadHandler", ipcMetrics.getNumOpenConnections());
            Sample numActiveScanHandler = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"numActiveScanHandler", ipcMetrics.getNumOpenConnections());
            familySamples.addSample(numOpenConnections)
                    .addSample(numCallsInWriteQueue)
                    .addSample(numCallsInReadQueue)
                    .addSample(numCallsInScanQueue)
                    .addSample(numActiveWriteHandler)
                    .addSample(numActiveReadHandler)
                    .addSample(numActiveScanHandler);

            if(!this.nodesSnapshot.containsKey(nodeMetrics.getIp())) continue;
            HBaseRsIpcMetrics snapshotIpcMetrics =
                    this.nodesSnapshot.get(nodeMetrics.getIp()).getIpcMetrics();
            if(snapshotIpcMetrics == null) continue;

            Sample callsCount = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"callsCount",
                    intervalValue(ipcMetrics.getCallsCount(), snapshotIpcMetrics.getCallsCount()));
            Sample exceptionsCount = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"exceptionsCount",
                    intervalValue(ipcMetrics.getExceptionsCount(), snapshotIpcMetrics.getExceptionsCount()));
            familySamples.addSample(callsCount).addSample(exceptionsCount);

            List<Sample> callsOfCalltime =
                    this.timeRangeSample(
                            familySamples.name, nodeMetrics.getIp(),
                            ipcMetrics.getCallsOfCalltime(), snapshotIpcMetrics.getCallsOfCalltime()
                    );
            List<Sample> callsOfProcesstime =
                    this.timeRangeSample(
                            familySamples.name, nodeMetrics.getIp(),
                            ipcMetrics.getCallsOfProcesstime(), snapshotIpcMetrics.getCallsOfProcesstime()
                    );
            List<Sample> callsOfQueuetime =
                    this.timeRangeSample(
                            familySamples.name, nodeMetrics.getIp(),
                            ipcMetrics.getCallsOfQueuetime(), snapshotIpcMetrics.getCallsOfQueuetime()
                    );
            familySamples.addSamples(callsOfCalltime)
                    .addSamples(callsOfProcesstime)
                    .addSamples(callsOfQueuetime);
        }
        return familySamples;
    }

    /**
     * 时间响应 监控信息
     * @param familyname
     * @param nodeId
     * @param current
     * @param snapshot
     * @return
     */
    private List<Sample> timeRangeSample(
            String familyname, String nodeId,
            Map<String, Long> current, Map<String, Long> snapshot) {
        List<Sample> samples = new ArrayList<>();
        for(Map.Entry<String, Long> kv: current.entrySet()) {
            Sample sample = this.getNodeSample(familyname,
                    nodeId, kv.getKey(),
                    intervalValue(kv.getValue(), snapshot.get(kv.getKey())));
            samples.add(sample);
        }
        return samples;
    }

    /**
     * jvm 相关监控信息
     * @param nodesMetrics
     * @return
     */
    private MetricFamilySamples jvmMetrics(List<HBaseRsMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_RS_JVM_METRICS.getKey(), HBASE_RS_JVM_METRICS.getValue());
        for(HBaseRsMetric nodeMetrics : nodesMetrics) {
            JvmMetrics jvmMetrics =  nodeMetrics.getJvmMetrics();
            if(jvmMetrics == null) continue;

            Sample memHeapUsedM = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"memHeapUsedM", jvmMetrics.getMemHeapUsedM());
            Sample runnableThreads = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"runnableThreads", jvmMetrics.getRunnableThreads());
            Sample blockedThreads = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"blockedThreads", jvmMetrics.getBlockedThreads());
            familySamples.addSample(memHeapUsedM)
                    .addSample(runnableThreads)
                    .addSample(blockedThreads);

            if(!this.nodesSnapshot.containsKey(nodeMetrics.getIp())) continue;
            JvmMetrics snapshotJvmMetrics =
                    this.nodesSnapshot.get(nodeMetrics.getIp()).getJvmMetrics();
            if(snapshotJvmMetrics == null) continue;
            Sample gcCount = this.getNodeSample(familySamples.name,
                    nodeMetrics.getIp(),"gcCount",
                    intervalValue(jvmMetrics.getGcCount(), snapshotJvmMetrics.getGcCount())
            );
            Sample gcTimeMillis = this.getNodeSample(familySamples.name,
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
    private MetricFamilySamples statusMetric(List<HBaseRsMetric> nodesMetrics) {
        MetricFamilySamples familySamples =
                MetricFamilySamples.build(HBASE_RS_STATUS.getKey(), HBASE_RS_STATUS.getValue());
        for(HBaseRsMetric nodeMetrics : nodesMetrics) {
            int status = nodeMetrics.getStatus().getStatusId();
            Sample sample = this.getNodeSample(familySamples.name, nodeMetrics.getIp(), status);
            familySamples.addSample(sample);
        }
        return familySamples;
    }

    private Sample getNodeSample(String familyname, String nodeId, double value) {
        return this.getNodeSample(familyname, nodeId, null, value);
    }

    private Sample getNodeSample(String familyname, String nodeId, String l2babel, double value) {
        Sample sample = Sample.build(familyname, serverConfig.getClusterId());
        sample.addLable("nodeId", nodeId);
        if(l2babel != null && l2babel.length() > 0) {
            sample.addLable("l2babel", l2babel);
        }
        sample.setSampleValue(value);
        return sample;
    }

    private Sample getTableSample(String familyname, String tablename, String l2babel, double value) {
        Sample sample = Sample.build(familyname, serverConfig.getClusterId());
        sample.addLable("tablename", tablename);
        if(l2babel != null && l2babel.length() > 0) {
            sample.addLable("l2babel", l2babel);
        }
        sample.setSampleValue(value);
        return sample;
    }

    private List<HBaseRsMetric> getHBaseRsMonitorInfo() {
        List<HBaseRsMetric> rsListMetrics = new ArrayList<>();
        for(ServerConfig.ServerNode node : this.regionservers) {
            rsListMetrics.add(HBaseRsMetric.parse(node.getIp(), node.getPort()));
        }
        return rsListMetrics;
    }

    private void updateShopshot(List<HBaseRsMetric> nodesMetrics) {
        nodesSnapshot = new HashMap<>();
        for(HBaseRsMetric node : nodesMetrics) {
            nodesSnapshot.put(node.getIp(), node);
        }
    }

    private Map<String, TableMetrics> getTablesMetrics(Collection<HBaseRsMetric> nodesMetrics) {
        Map<String, TableMetrics> retTables = new HashMap<>();
        if(nodesMetrics == null) return retTables;
        for(HBaseRsMetric nodeMetrics : nodesMetrics) {
            if(!nodeMetrics.getStatus().equalTo(StatusEnum.ONLINE)) continue;
            // usable merged(multi tables) metrics
            Map<String, HBaseRsTableMetrics> tablesMetrics = nodeMetrics.getTablesMetrics();
            for(HBaseRsTableMetrics t : tablesMetrics.values()) {
                String tablename = t.getTablename();
                if(!retTables.containsKey(tablename)) {
                    retTables.put(tablename, new TableMergedMetrics(tablename));
                }
                TableMergedMetrics tableMetrics = (TableMergedMetrics) retTables.get(tablename);
                Map<String, HBaseRsRegionMetrics> regions = t.getRegionsMetrics();
                tableMetrics.regionCount += t.getRegionsMetrics().size();
                for(HBaseRsRegionMetrics region : regions.values()) {
                    tableMetrics.storeFileSize += region.getStoreFileSize();
                    tableMetrics.storeFileCount += region.getStoreFileCount();
                    if(region.getStoreFileSize() / (1024 * 1024) > serverConfig.getGbOfRegionSizeLimit()){
                        tableMetrics.tooBigRegionCount += 1;
                    }
                    if(region.getStoreFileCount() > serverConfig.getNumberOfRegionHfilesLimit()){
                        tableMetrics.blockedRegionCount += 1;
                    }
                }
            }

            // unusable merged(multi tables) metrics
            for(HBaseRsTableMetrics t : tablesMetrics.values()) {
                String tablename = t.getTablename() + "::" + nodeMetrics.getIp();
                retTables.put(tablename, new TableUnMergedMetrics(tablename));
                TableUnMergedMetrics tableMetrics = (TableUnMergedMetrics) retTables.get(tablename);
                tableMetrics.scanOps = t.getScanOps();
                tableMetrics.incrementOps = t.getIncrementOps();
                tableMetrics.batchDeleteOps = t.getBatchDeleteOps();
                tableMetrics.batchPutOps = t.getBatchPutOps();
                tableMetrics.putOps = t.getPutOps();
                tableMetrics.deleteOps = t.getDeleteOps();
                tableMetrics.appendOps = t.getAppendOps();
                tableMetrics.compactionOps = t.getCompactionOps();
                tableMetrics.getsOfProcesstime = t.getGetsOfProcesstime();
            }
        }
        return retTables;
    }

    private abstract class TableMetrics {
        String tablename;
        private TableMetrics(String tablename) {
            this.tablename = tablename;
        }
    }

    private class TableMergedMetrics extends TableMetrics{
        Long regionCount = 0L;

        Long storeFileCount = 0L;

        Long blockedRegionCount = 0L;

        Long storeFileSize = 0L;

        Long tooBigRegionCount = 0L;

        private TableMergedMetrics(String tablename) {
            super(tablename);
        }

        Long getStoreFileSizeGB() {
            return this.storeFileSize / (1024* 1024);
        }

    }

    private class TableUnMergedMetrics extends TableMetrics{

        Long scanOps = 0L;

        Long incrementOps = 0L;

        Long deleteOps = 0L;

        Long putOps = 0L;

        Long batchDeleteOps = 0L;

        Long batchPutOps = 0L;

        Long appendOps = 0L;

        Long compactionOps = 0L;

        Map<String, Long> getsOfProcesstime = new HashMap<>();

        private TableUnMergedMetrics(String tablename) {
            super(tablename);
        }
    }
}
