package org.apache.hadoop.metrics2.impl;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsFilter;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * @author zhengzhubin
 * @date 2020/3/31
 * @description
 **/
public class YjMetricsRecordBuilderUtil {

    public static MetricsRecordBuilder getMetricsRecordBuilder(String name) {
        MetricsCollector collector = new MetricsCollectorImpl();
        return collector.addRecord(name);
    }

}
