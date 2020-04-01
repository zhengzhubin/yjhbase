package org.apache.hadoop.metrics2.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.hadoop.metrics2.lib.YjMutableTimeHistogram;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * @author zhengzhubin
 * @date 2020/3/31
 * @description
 **/
public class TestMutableTimeHistogram {

    private static ObjectMapper objectMapper = new ObjectMapper();
    static Random random = new Random();
    @Test
    public void test() throws JsonProcessingException {
        MetricsRecordBuilderImpl metricsRecordBuilder =
                (MetricsRecordBuilderImpl) YjMetricsRecordBuilderUtil.getMetricsRecordBuilder("test");

        MetricHistogram totalCallTime =
                new YjMutableTimeHistogram("totalCallTime", "regionserver ipc");

        int x = 10000;
        for(int i = 0; i< 3 ;i++) {
            test(metricsRecordBuilder, totalCallTime, x);
            System.out.println("============================================================");
            x = x / 10;
        }

    }


    private void test(MetricsRecordBuilderImpl metricsRecordBuilder, MetricHistogram totalCallTime,
                      int limit) throws JsonProcessingException {


        for(int i = 0; i< 10000;i ++) {
            totalCallTime.add(random.nextInt(limit));
        }
        ((MutableTimeHistogram) totalCallTime).snapshot(metricsRecordBuilder, false);

        List<AbstractMetric> metrics = metricsRecordBuilder.metrics();
        for(AbstractMetric m : metrics) {
            System.out.println(// "clazz => " + m.getClass().getName() + " , " +
                    "name = " + m.name() + ", value = " + m.value());
        }

    }



}
