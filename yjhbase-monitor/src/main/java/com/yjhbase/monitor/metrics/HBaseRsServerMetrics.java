package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.utils.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/4/1
 * @description
 **/
public class HBaseRsServerMetrics extends RangeMetric{
    private static Logger LOG = LoggerFactory.getLogger(HBaseRsMetric.class);

    Long regionCount;

    Long storeFileCount;

    //storeFileSize(B)
    Long storeFileSizeGB;

    Long splitQueueLength;

    Long compactionQueueLength;

    Long smallCompactionQueueLength;

    Long largeCompactionQueueLength;

    Long flushQueueLength;

    Long totalRequestCount;

    Long readRequestCount;

    Long writeRequestCount;

    Long rpcGetRequestCount;

    Long rpcScanRequestCount;

    Long rpcMultiRequestCount;

    Long rpcMutateRequestCount;

    Long flushedCellsCount;

    Long compactedCellsCount;

    Long majorCompactedCellsCount;

    Long flushedCellsSize;

    Long compactedCellsSize;

    Long majorCompactedCellsSize;

    Long blockedRequestCount;

    //MajorCompactionTime_num_ops
    Long majorCompactionOps;

    //CompactionTime_num_ops
    Long compactionOps;

    //FlushTime_num_ops
    Long flushOps;

    //SplitTime_num_ops
    Long splitOps;

    // ScanTime_num_ops
    Map<String, Long> scansOfProcesstime = new HashMap<>();

    // Increment_num_ops
    Map<String, Long> incrementsOfProcesstime = new HashMap<>();

    // Delete_num_ops
    Map<String, Long> deletesOfProcesstime = new HashMap<>();

    // Put_num_ops
    Map<String, Long> putsOfProcesstime = new HashMap<>();

    // DeleteBatch_num_ops
    Map<String, Long> batchDeletesOfProcesstime = new HashMap<>();

    // PutBatch_num_ops
    Map<String, Long> batchPutsOfProcesstime = new HashMap<>();

    // Append_num_ops
    Map<String, Long> appendsOfProcesstime = new HashMap<>();

    // Get_num_ops
    Map<String, Long> getsOfProcesstime = new HashMap<>();

    private HBaseRsServerMetrics() { }

    public static HBaseRsServerMetrics parse(String json) {
        try {
            Map<String, Object> dataMap =
                    HBaseRsMetric.parseRegionserverMetric(json,
                            "Hadoop:service=HBase,name=RegionServer,sub=Server");
            HBaseRsServerMetrics serverMetrics = new HBaseRsServerMetrics();
            serverMetrics.regionCount = DataUtils.longValue(dataMap, "regionCount", 0);
            serverMetrics.storeFileCount = DataUtils.longValue(dataMap, "storeFileCount", 0);
            serverMetrics.storeFileSizeGB = DataUtils.longValue(dataMap, "storeFileSize", 0)/(1024 * 1024);
            serverMetrics.splitQueueLength = DataUtils.longValue(dataMap, "splitQueueLength", 0);
            serverMetrics.compactionQueueLength = DataUtils.longValue(dataMap, "compactionQueueLength", 0);
            serverMetrics.smallCompactionQueueLength = DataUtils.longValue(dataMap, "smallCompactionQueueLength", 0);
            serverMetrics.largeCompactionQueueLength = DataUtils.longValue(dataMap, "largeCompactionQueueLength", 0);
            serverMetrics.flushQueueLength = DataUtils.longValue(dataMap, "flushQueueLength", 0);
            serverMetrics.totalRequestCount = DataUtils.longValue(dataMap, "totalRequestCount", 0);
            serverMetrics.writeRequestCount = DataUtils.longValue(dataMap, "writeRequestCount", 0);
            serverMetrics.readRequestCount = DataUtils.longValue(dataMap, "readRequestCount", 0);
            serverMetrics.rpcGetRequestCount = DataUtils.longValue(dataMap, "rpcGetRequestCount", 0);
            serverMetrics.rpcScanRequestCount = DataUtils.longValue(dataMap, "rpcScanRequestCount", 0);
            serverMetrics.rpcMultiRequestCount = DataUtils.longValue(dataMap, "rpcMultiRequestCount", 0);
            serverMetrics.rpcMutateRequestCount = DataUtils.longValue(dataMap, "rpcMutateRequestCount", 0);
            serverMetrics.flushedCellsCount = DataUtils.longValue(dataMap, "flushedCellsCount", 0);
            serverMetrics.compactedCellsCount = DataUtils.longValue(dataMap, "compactedCellsCount", 0);
            serverMetrics.majorCompactedCellsCount = DataUtils.longValue(dataMap, "majorCompactedCellsCount", 0);
            serverMetrics.flushedCellsSize = DataUtils.longValue(dataMap, "flushedCellsSize", 0);
            serverMetrics.compactedCellsSize = DataUtils.longValue(dataMap, "compactedCellsSize", 0);
            serverMetrics.majorCompactedCellsSize = DataUtils.longValue(dataMap, "majorCompactedCellsSize", 0);
            serverMetrics.blockedRequestCount = DataUtils.longValue(dataMap, "blockedRequestCount", 0);
            serverMetrics.majorCompactionOps = DataUtils.longValue(dataMap, "MajorCompactionTime_num_ops", 0);
            serverMetrics.compactionOps = DataUtils.longValue(dataMap, "CompactionTime_num_ops", 0);
            serverMetrics.flushOps = DataUtils.longValue(dataMap, "FlushTime_num_ops", 0);
            serverMetrics.splitOps = DataUtils.longValue(dataMap, "SplitTime_num_ops", 0);

            serverMetrics.getsOfProcesstime =
                    serverMetrics.getRanges(dataMap, "Get_yjTimeRangeCount", "getsOfProcesstime", false);
            serverMetrics.putsOfProcesstime =
                    serverMetrics.getRanges(dataMap, "Put_yjTimeRangeCount", "putsOfProcesstime", false);
            serverMetrics.deletesOfProcesstime =
                    serverMetrics.getRanges(dataMap, "Delete_yjTimeRangeCount", "deletesOfProcesstime", false);
            serverMetrics.appendsOfProcesstime =
                    serverMetrics.getRanges(dataMap, "Append_yjTimeRangeCount", "appendsOfProcesstime", false);
            serverMetrics.incrementsOfProcesstime =
                    serverMetrics.getRanges(dataMap, "Increment_yjTimeRangeCount", "incrementsOfProcesstime", false);
            serverMetrics.batchPutsOfProcesstime =
                    serverMetrics.getRanges(dataMap, "PutBatch_yjTimeRangeCount", "batchPutsOfProcesstime", false);
            serverMetrics.batchDeletesOfProcesstime =
                    serverMetrics.getRanges(dataMap, "DeleteBatch_yjTimeRangeCount", "batchDeletesOfProcesstime", false);
            serverMetrics.scansOfProcesstime =
                    serverMetrics.getRanges(dataMap, "ScanTime_yjTimeRangeCount", "scansOfProcesstime", false);

            return serverMetrics;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    public Long getRegionCount() {
        return regionCount;
    }

    public Long getStoreFileCount() {
        return storeFileCount;
    }

    public Long getStoreFileSizeGB() {
        return storeFileSizeGB;
    }

    public Long getSplitQueueLength() {
        return splitQueueLength;
    }

    public Long getCompactionQueueLength() {
        return compactionQueueLength;
    }

    public Long getSmallCompactionQueueLength() {
        return smallCompactionQueueLength;
    }

    public Long getLargeCompactionQueueLength() {
        return largeCompactionQueueLength;
    }

    public Long getFlushQueueLength() {
        return flushQueueLength;
    }

    public Long getTotalRequestCount() {
        return totalRequestCount;
    }

    public Long getReadRequestCount() {
        return readRequestCount;
    }

    public Long getWriteRequestCount() {
        return writeRequestCount;
    }

    public Long getRpcGetRequestCount() {
        return rpcGetRequestCount;
    }

    public Long getRpcScanRequestCount() {
        return rpcScanRequestCount;
    }

    public Long getRpcMultiRequestCount() {
        return rpcMultiRequestCount;
    }

    public Long getRpcMutateRequestCount() {
        return rpcMutateRequestCount;
    }

    public Long getFlushedCellsCount() {
        return flushedCellsCount;
    }

    public Long getCompactedCellsCount() {
        return compactedCellsCount;
    }

    public Long getMajorCompactedCellsCount() {
        return majorCompactedCellsCount;
    }

    public Long getFlushedCellsSize() {
        return flushedCellsSize;
    }

    public Long getCompactedCellsSize() {
        return compactedCellsSize;
    }

    public Long getMajorCompactedCellsSize() {
        return majorCompactedCellsSize;
    }

    public Long getBlockedRequestCount() {
        return blockedRequestCount;
    }

    public Long getMajorCompactionOps() {
        return majorCompactionOps;
    }

    public Long getCompactionOps() {
        return compactionOps;
    }

    public Long getFlushOps() {
        return flushOps;
    }

    public Long getSplitOps() {
        return splitOps;
    }

    public Map<String, Long> getScansOfProcesstime() {
        return scansOfProcesstime;
    }

    public Map<String, Long> getIncrementsOfProcesstime() {
        return incrementsOfProcesstime;
    }

    public Map<String, Long> getDeletesOfProcesstime() {
        return deletesOfProcesstime;
    }

    public Map<String, Long> getPutsOfProcesstime() {
        return putsOfProcesstime;
    }

    public Map<String, Long> getBatchDeletesOfProcesstime() {
        return batchDeletesOfProcesstime;
    }

    public Map<String, Long> getBatchPutsOfProcesstime() {
        return batchPutsOfProcesstime;
    }

    public Map<String, Long> getAppendsOfProcesstime() {
        return appendsOfProcesstime;
    }

    public Map<String, Long> getGetsOfProcesstime() {
        return getsOfProcesstime;
    }
}
