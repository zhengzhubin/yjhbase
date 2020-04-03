package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.dto.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/4/1
 * @description
 **/
public abstract class RangeMetric {

    private static long[] timeRanges =
            new long[]{0L, 10L, 30L, 100L, 300L, 1000L, 3000L, 10000L, 30000L, 60000L};

    private static long[] queueTimeRanges =
            new long[]{1L, 3L, 10L, 30L, 100L, 300L, 1000L, 3000L, 10000L};


    protected String parseMetricName(String metric, long time) {
        return metric +"Gte" + time;
    }

    protected Map<String, Long> getRanges(Map<String, Object> dataMap,
                                          String prefix, String metric, boolean isQueuetime) {
        Map<String, Long> retRangeMap = new HashMap<>();
        List<Pair<Long, Long>> rangeKVs = getRanges(dataMap, prefix);
        long[] timeRages = (isQueuetime ? queueTimeRanges : timeRanges);
        for(Long t : timeRages) {
            String name = this.parseMetricName(metric , t);
            long countv = 0L;
            for(Pair<Long, Long> rangeKV : rangeKVs) {
                if(rangeKV.getKey() < t) continue;
                countv += rangeKV.getValue();
            }
            retRangeMap.put(name, countv);
        }
        return retRangeMap;
    }

    private List<Pair<Long, Long>> getRanges(Map<String, Object> dataMap, String prefix) {
        List<Pair<Long, Long>> retRanges = new ArrayList<>();
        for(Map.Entry<String, Object> kv : dataMap.entrySet()) {
            if(!kv.getKey().startsWith(prefix)) continue;
            String[] infos = kv.getKey().split("_");
            Long time = Long.parseLong(infos[infos.length - 2]);
            Long countv = Long.parseLong(kv.getValue() + "");
            retRanges.add(new Pair<>(time, countv));
        }
        return retRanges;
    }
}
