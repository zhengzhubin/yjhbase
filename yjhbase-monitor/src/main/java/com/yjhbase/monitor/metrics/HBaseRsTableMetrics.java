package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.utils.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author zhengzhubin
 * @date 2020/4/1
 * @description
 **/
public class HBaseRsTableMetrics extends RangeMetric {

    private static Logger LOG = LoggerFactory.getLogger(HBaseRsMetric.class);

    String tablename;

    Map<String, HBaseRsRegionMetrics> regionsMetrics = new HashMap<>();

    // ScanTime_num_ops
//    Map<String, Long> scansOfProcesstime = new HashMap<>();
    Long scanOps;

    // Increment_num_ops
//    Map<String, Long> incrementsOfProcesstime = new HashMap<>();
    Long incrementOps;

    // Delete_num_ops
//    Map<String, Long> deletesOfProcesstime = new HashMap<>();
    Long deleteOps;

    // Put_num_ops
//    Map<String, Long> putsOfProcesstime = new HashMap<>();
    Long putOps;

    // DeleteBatch_num_ops
//    Map<String, Long> batchDeletesOfProcesstime = new HashMap<>();
    Long batchDeleteOps;

    // PutBatch_num_ops
//    Map<String, Long> batchPutsOfProcesstime = new HashMap<>();
    Long batchPutOps;

    // Append_num_ops
//    Map<String, Long> appendsOfProcesstime = new HashMap<>();
    Long appendOps;

    // Get_num_ops
    Map<String, Long> getsOfProcesstime = new HashMap<>();

    private HBaseRsTableMetrics(String tablename) {
        this.tablename = tablename;
    }

    public static HBaseRsTableMetrics parse(
            String tablename,
            Map<String, Object> regionsDataMap, Map<String, Object> tableLatenciesDataMap) {
        HBaseRsTableMetrics tableMetrics = new HBaseRsTableMetrics(tablename);
        return tableMetrics.fillTableLatenciesMetrics(tableLatenciesDataMap)
                .fillRegionsMetrics(regionsDataMap);
    }

    /**
     * regions 相关监控信息
     * @param dataMap
     * @return
     */
    private HBaseRsTableMetrics fillRegionsMetrics(Map<String, Object> dataMap) {
        this.regionsMetrics = new HashMap<>();
        Set<String> regionsId = this.getRegions(this.tablename, dataMap);
        for(String regionId : regionsId) {
            this.regionsMetrics.put(regionId, HBaseRsRegionMetrics.parse(regionId, dataMap));
        }
        return this;
    }

    /**
     * 获取表下的regions
     * @param tablename
     * @param regionsDataMap
     * @return
     */
    private Set<String> getRegions(String tablename, Map<String, Object> regionsDataMap) {
        Set<String> retRegions = new HashSet<>();
        String prefix = "Namespace_"+tablename.replace(":", "_table_")+"_region_";
        for(Map.Entry<String, Object> kv : regionsDataMap.entrySet()) {
            if(!kv.getKey().startsWith(prefix)) continue;
            String regionId = kv.getKey().substring(0, kv.getKey().lastIndexOf("_metric_"));
            if(retRegions.contains(regionId)) continue;
            retRegions.add(regionId);
        }
        return retRegions;
    }

    /**
     * 表请求 相关监控信息
     * @param dataMap
     * @return
     */
    private HBaseRsTableMetrics fillTableLatenciesMetrics(Map<String, Object> dataMap) {
        String prefix = "Namespace_"+tablename.replace(":", "_table_")+"_metric_";

        this.scanOps = DataUtils.longValue(dataMap, prefix + "scanTime_num_ops", 0);
        this.incrementOps = DataUtils.longValue(dataMap, prefix + "incrementTime_num_ops", 0);
        this.deleteOps = DataUtils.longValue(dataMap, prefix + "deleteTime_num_ops", 0);
        this.putOps = DataUtils.longValue(dataMap, prefix + "putTime_num_ops", 0);
        this.batchDeleteOps = DataUtils.longValue(dataMap, prefix + "deleteBatchTime_num_ops", 0);
        this.batchPutOps = DataUtils.longValue(dataMap, prefix + "putBatchTime_num_ops", 0);
        this.appendOps = DataUtils.longValue(dataMap, prefix + "appendTime_num_ops", 0);

        this.getsOfProcesstime =
                this.getRanges(dataMap, prefix + "getTime_yjTimeRangeCount", "getsOfProcesstime", false);

        return this;
    }

    public String getTablename() {
        return tablename;
    }

    public Map<String, HBaseRsRegionMetrics> getRegionsMetrics() {
        return regionsMetrics;
    }

    public Long getScanOps() {
        return scanOps;
    }

    public Long getIncrementOps() {
        return incrementOps;
    }

    public Long getDeleteOps() {
        return deleteOps;
    }

    public Long getPutOps() {
        return putOps;
    }

    public Long getBatchDeleteOps() {
        return batchDeleteOps;
    }

    public Long getBatchPutOps() {
        return batchPutOps;
    }

    public Long getAppendOps() {
        return appendOps;
    }

    public Map<String, Long> getGetsOfProcesstime() {
        return getsOfProcesstime;
    }

    public Long getCompactionOps() {
        long compactionOps = 0L;
        for(HBaseRsRegionMetrics region : this.getRegionsMetrics().values()) {
            compactionOps += region.getCompactionOps();
        }

        return compactionOps;
    }
}
