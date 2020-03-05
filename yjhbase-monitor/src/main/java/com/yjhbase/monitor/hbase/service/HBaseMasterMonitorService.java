package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.metrics.HBaseMasterMetric;

import java.util.List;

public interface HBaseMasterMonitorService {

    List<HBaseMasterMetric> getHBaseMastersMetric();

}
