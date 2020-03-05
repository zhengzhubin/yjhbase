package com.yjhbase.monitor.hdfs.service;

import com.yjhbase.monitor.metrics.HdfsNamenodeMetric;

import java.util.List;

public interface HdfsNamenodeMonitorService {

    List<HdfsNamenodeMetric> getHdfsNamenodesMetric();

}
