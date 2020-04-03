package com.yjhbase.monitor.hdfs.service;

import com.yjhbase.monitor.dto.Pair;
import com.yjhbase.monitor.metrics.HdfsNamenodeMetric;

import java.io.Writer;
import java.util.List;

public interface HdfsNamenodeMonitorService {

    // Pair<$metric.name, $metric.description>
    static Pair<String, String> HDFS_NN_STATUS =
            new Pair<>("hdfsNamenodeStatus", "hdfs namenode status metric.");

    static Pair<String, String> HDFS_JVM_METRICS =
            new Pair<>("hdfsNamenodeJvmMetrics", "hdfs namenode jvm metrics.");

    static Pair<String, String> HDFS_INFO_METRICS =
            new Pair<>("hdfsNamenodeInfoMetrics", "hdfs namenode info metrics.");


    void buildHdfsnamenodeMonitorInfoWithPrometheusFormat(Writer writer) throws Exception;

}
