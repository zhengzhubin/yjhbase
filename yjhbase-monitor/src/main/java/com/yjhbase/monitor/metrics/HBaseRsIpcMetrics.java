package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.utils.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/30
 * @description
 **/
public class HBaseRsIpcMetrics extends RangeMetric {

    private static Logger LOG = LoggerFactory.getLogger(HBaseRsMetric.class);

    //TotalCallTime_num_ops
    Long callsCount;

    Long numOpenConnections;

    Long numCallsInWriteQueue;

    Long numCallsInReadQueue;

    Long numCallsInScanQueue;

    Long numActiveWriteHandler;

    Long numActiveReadHandler;

    Long numActiveScanHandler;

    Long exceptionsCount;

    //time unit: ms
    Map<String, Long> callsOfCalltime = new HashMap<>();

    Map<String, Long> callsOfProcesstime = new HashMap<>();

    Map<String, Long> callsOfQueuetime = new HashMap<>();

    private HBaseRsIpcMetrics() { }

    public static HBaseRsIpcMetrics parse(String json) {
        try {
            Map<String, Object> dataMap =
                    HBaseRsMetric.parseRegionserverMetric(json,
                            "Hadoop:service=HBase,name=RegionServer,sub=IPC");
            HBaseRsIpcMetrics ipcMetrics = new HBaseRsIpcMetrics();
            ipcMetrics.callsCount = DataUtils.longValue(dataMap, "TotalCallTime_num_ops", 0);
            ipcMetrics.numOpenConnections = DataUtils.longValue(dataMap, "numOpenConnections", 0);
            ipcMetrics.exceptionsCount = DataUtils.longValue(dataMap, "exceptions", 0);
            ipcMetrics.numActiveReadHandler = DataUtils.longValue(dataMap, "numActiveReadHandler", 0);
            ipcMetrics.numActiveScanHandler = DataUtils.longValue(dataMap, "numActiveScanHandler", 0);
            ipcMetrics.numActiveWriteHandler = DataUtils.longValue(dataMap, "numActiveWriteHandler", 0);
            ipcMetrics.numCallsInReadQueue = DataUtils.longValue(dataMap, "numCallsInReadQueue", 0);
            ipcMetrics.numCallsInScanQueue = DataUtils.longValue(dataMap, "numCallsInScanQueue", 0);
            ipcMetrics.numCallsInWriteQueue = DataUtils.longValue(dataMap, "numCallsInWriteQueue", 0);
            ipcMetrics.callsOfCalltime =
                    ipcMetrics.getRanges(dataMap, "QueueCallTime_yjTimeRangeCount", "callsOfTotaltime", false);
            ipcMetrics.callsOfProcesstime =
                    ipcMetrics.getRanges(dataMap, "ProcessCallTime_yjTimeRangeCount", "callsOfProcesstime", false);
            ipcMetrics.callsOfQueuetime =
                    ipcMetrics.getRanges(dataMap, "QueueCallTime_yjTimeRangeCount", "callsOfQueuetime", true);
            return ipcMetrics;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }

    public Long getCallsCount() {
        return callsCount;
    }

    public Long getNumOpenConnections() {
        return numOpenConnections;
    }

    public Long getNumCallsInWriteQueue() {
        return numCallsInWriteQueue;
    }

    public Long getNumCallsInReadQueue() {
        return numCallsInReadQueue;
    }

    public Long getNumCallsInScanQueue() {
        return numCallsInScanQueue;
    }

    public Long getNumActiveWriteHandler() {
        return numActiveWriteHandler;
    }

    public Long getNumActiveReadHandler() {
        return numActiveReadHandler;
    }

    public Long getNumActiveScanHandler() {
        return numActiveScanHandler;
    }

    public Long getExceptionsCount() {
        return exceptionsCount;
    }

    public Map<String, Long> getCallsOfCalltime() {
        return callsOfCalltime;
    }

    public Map<String, Long> getCallsOfProcesstime() {
        return callsOfProcesstime;
    }

    public Map<String, Long> getCallsOfQueuetime() {
        return callsOfQueuetime;
    }
}
