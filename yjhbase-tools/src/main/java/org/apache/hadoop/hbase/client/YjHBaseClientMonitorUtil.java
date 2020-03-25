package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/24
 * @description
 **/
public class YjHBaseClientMonitorUtil {

    private static Logger LOG = LoggerFactory.getLogger(YjHBaseClientMonitorUtil.class);

    private static long requests = 0L, successRequests = 0L, failedRequests = 0L;

    private static Map<String, Long> successRequestsRpcTookInMillisMap = new HashMap<>();
    private static Map<String, Long> successRequestsWaitInMillisMap = new HashMap<>();
    private static Map<String, Long> failedRequestsWaitInMillisMap = new HashMap<>();

    public static synchronized void incrRequest() {
        requests ++;
    }

    /**
     * 成功请求统计
     * @param waitTimeInMillis 请求在队列等待时间
     * @param rpcTookTimeInMillis 请求rpc 耗时
     */
    public static synchronized void incrSuccessRequest(long waitTimeInMillis, long rpcTookTimeInMillis) {
        successRequests ++;
        parse(successRequestsWaitInMillisMap, waitTimeInMillis);
        parse(successRequestsRpcTookInMillisMap, rpcTookTimeInMillis);
        printlnSuccessRequsts();
    }

    private static long successfulCurrent = EnvironmentEdgeManager.currentTime();
    private static void printlnSuccessRequsts() {
        if(successRequests % 1000 == 1 ||
                EnvironmentEdgeManager.currentTime() - successfulCurrent > 6000L) {
            successfulCurrent = EnvironmentEdgeManager.currentTime();
            println("successRequestsWaitInMillisMap", successRequestsWaitInMillisMap, successRequests);
            println("successRequestsRpcTookInMillisMap", successRequestsRpcTookInMillisMap, successRequests);
        }
    }

    /**
     * 失败请求统计
     * @param waitTimeInMillis 请求在队列等待时间
     */
    public static synchronized void incrFailedRequest(long waitTimeInMillis) {
        failedRequests ++;
        parse(failedRequestsWaitInMillisMap, waitTimeInMillis);
        printlnFailedRequsts();
    }

    private static long failedCurrent = EnvironmentEdgeManager.currentTime();
    private static void printlnFailedRequsts() {
        if(failedRequests % 1000 == 1 ||
                EnvironmentEdgeManager.currentTime() - failedCurrent > 6000L) {
            failedCurrent = EnvironmentEdgeManager.currentTime();
            println("failedRequestsWaitInMillisMap", failedRequestsWaitInMillisMap, failedRequests);
        }
    }

    private static void println(String metric, Map<String, Long> map, long requests) {
        String retString = "\nHBase 请求耗时情况[" +metric+ "], requests = "+ requests +": \n";
        for(Map.Entry<String, Long> kv: map.entrySet()) {
            retString = retString + "耗时：" + kv.getKey() + " , 请求数: " + kv.getValue() + " \n";
        }
        LOG.info(retString);
    }

    private static void parse(Map<String, Long> map, long millis) {
        String key = parse(millis);
        Long value = (map.containsKey(key) ? map.get(key) : 0L);
        map.put(key, value + 1);
    }

    private static String parse(long millis) {
        if(millis < 100) {
            Long key = (millis / 10);
            return key*10 + "~" + (key + 1) * 10 + "ms";
        } else if(millis < 1000) {
            Long key = millis / 100;
            return key*100 + "~" + (key + 1) * 100 + "ms";
        } else {
            return "1000ms+";
        }
    }

}
