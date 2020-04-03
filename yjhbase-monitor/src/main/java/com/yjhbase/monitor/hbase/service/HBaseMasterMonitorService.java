package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.dto.Pair;
import com.yjhbase.monitor.metrics.HBaseMasterMetric;

import java.io.Writer;
import java.util.List;

public interface HBaseMasterMonitorService {

    // Pair<$metric.name, $metric.description>
    static Pair<String, String> HBASE_MASTER_STATUS =
            new Pair<>("hbaseMasterStatus", "hbase master status metric.");

    static Pair<String, String> HBASE_MASTER_JVM_METRICS =
            new Pair<>("hbaseMasterJvmMetrics", "hbase master jvm metrics.");

    void buildHBaseMasterMonitorInfoWithPrometheusFormat(Writer writer) throws Exception;

}
