package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.dto.Pair;
import com.yjhbase.monitor.metrics.HBaseRsMetric;
import com.yjhbase.monitor.metrics.HBaseTableMetric;

import java.io.Writer;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public interface HBaseRsMonitorService {

    // Pair<$metric.name, $metric.description>
    static Pair<String, String> HBASE_RS_STATUS =
            new Pair<>("hbaseRsStatus", "hbase regionserver status metric.");

    static Pair<String, String> HBASE_RS_JVM_METRICS =
            new Pair<>("hbaseRsJvmMetrics", "hbase regionserver jvm metrics.");

    static Pair<String, String> HBASE_RS_IPC_METRICS =
            new Pair<>("hbaseRsIpcMetrics", "hbase regionserver ipc metrics.");

    static Pair<String, String> HBASE_RS_TABLES_METRICS =
            new Pair<>("hbaseRsTablesMetrics", "hbase regionserver tables metrics.");

    void buildHBaseRsMonitorInfoWithPrometheusFormat(Writer writer) throws Exception;

}
