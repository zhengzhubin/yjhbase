package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.metrics.HBaseRegionserverMetric;
import com.yjhbase.monitor.metrics.HBaseTableMetric;

import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public interface HBaseRegionserverMonitorService {

    List<HBaseRegionserverMetric> getHBaseRegionserversMetric();

    List<HBaseTableMetric> getHBaseTablesMetric();

}
