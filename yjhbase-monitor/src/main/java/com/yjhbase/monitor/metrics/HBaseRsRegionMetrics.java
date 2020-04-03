package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.utils.DataUtils;

import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/4/2
 * @description
 **/
public class HBaseRsRegionMetrics {

    String regionId;

    Long storeFileCount;

    Long storeFileSize;

    // compactionsCompletedCount
    Long compactionOps;

    private HBaseRsRegionMetrics(String regionId) {
        this.regionId = regionId;
    }

    public static HBaseRsRegionMetrics parse(String regionId, Map<String, Object> dataMap){
        HBaseRsRegionMetrics regionMetrics = new HBaseRsRegionMetrics(regionId);
        String prefix = regionId + "_metric_";
        regionMetrics.storeFileCount = DataUtils.longValue(dataMap, prefix + "storeFileCount", 0);
        regionMetrics.storeFileSize = DataUtils.longValue(dataMap, prefix + "storeFileSize", 0);
        regionMetrics.compactionOps = DataUtils.longValue(dataMap, prefix + "compactionsCompletedCount", 0);
        return regionMetrics;
    }

    public String getRegionId() {
        return regionId;
    }

    public Long getStoreFileCount() {
        return storeFileCount;
    }

    public Long getStoreFileSize() {
        return storeFileSize;
    }

    public Long getCompactionOps() {
        return compactionOps;
    }
}
