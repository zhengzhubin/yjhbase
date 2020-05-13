package com.yjhbase.tools.test.jobs.consumers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description
 **/
public class SummaryTool {

    static Logger LOG = LoggerFactory.getLogger(SummaryTool.class);

    static Long successRequests = 0L;
    static Long failedRequests = 0L;
    static Map<String, Long> spentMsDistribution = new HashMap<>();
    static Long requests = 0L;

    public static synchronized void requests(String consumerId){
        requests ++;
        if(requests % 10000 == 1) {
            LOG.info("requests = " + requests + ", consumerId = " + consumerId);
        }
    }

    public static synchronized void success(Long spentMS, String rkString) {
        successRequests ++;
        String key = distributeId(spentMS);
        if(!spentMsDistribution.containsKey(key)) {
            spentMsDistribution.put(key, 1L);
        } else {
            spentMsDistribution.put(key, spentMsDistribution.get(key) + 1);
        }
        println(rkString);
    }


    static  long lastMS = System.currentTimeMillis();
    //打印结果
    private static void println(String rkString) {
        if(System.currentTimeMillis() - lastMS < 60000 && successRequests % 10000 != 0) { // 1分钟打印1次
            return;
        }
        lastMS = System.currentTimeMillis();
        LOG.info("[rowkey = "+rkString+"] success requests: " + successRequests + ", failed requests: " + failedRequests);
        for(Map.Entry<String, Long> kv: spentMsDistribution.entrySet()) {
            LOG.info(kv.getKey() + " % : " + String.format("%.2f", kv.getValue()* 100.0/ successRequests));
        }
    }

    static String distributeId(Long spentMS) {
        if(spentMS < 100) {
            Long key = (spentMS / 10);
            return key*10 + "~" + (key + 1) * 10 + "ms";
        } else if(spentMS < 1000) {
            Long key = spentMS / 100;
            return key*100 + "~" + (key + 1) * 100 + "ms";
        } else {
            return "1000ms+";
        }
    }

    public static synchronized void failed(Exception e) {
        failedRequests ++;
        if(failedRequests % 10000 == 1) {
            LOG.error("request faield.", e);
            e.printStackTrace();
        }
    }
}
