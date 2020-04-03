package com.yjhbase.monitor.metrics;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author zhengzhubin
 * @date 2020/3/30
 * @description
 **/
public class JvmMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(JvmMetrics.class);

    //堆内存使用情况
    Float memHeapUsedM;

    // gc次数
    Long gcCount;

    //gc 累计耗时
    Long gcTimeMillis;

    //ThreadsRunnable
    Long runnableThreads;

    //ThreadsBlocked
    Long blockedThreads;

    private JvmMetrics() {}

    public static JvmMetrics parseFromHdfsNamenode(String json) {
        try {
            HashMap jsonMap = JSONObject.parseObject(json, HashMap.class);
            HashMap dataMap =
                    JSONObject.parseArray(
                            JSONObject.toJSONString(jsonMap.get("beans")),
                            HashMap.class
                    ).get(0);
            JvmMetrics jvmMetrics = new JvmMetrics();
            jvmMetrics.setMemHeapUsedM(Float.parseFloat(dataMap.get("MemHeapUsedM") + ""));
            jvmMetrics.setGcCount(Long.parseLong(dataMap.get("GcCount") + ""));
            jvmMetrics.setGcTimeMillis(Long.parseLong(dataMap.get("GcTimeMillis") + ""));
            jvmMetrics.setRunnableThreads(Long.parseLong(dataMap.get("ThreadsRunnable") + ""));
            jvmMetrics.setBlockedThreads(Long.parseLong(dataMap.get("ThreadsBlocked") + ""));
            return jvmMetrics;
        }catch (Exception e) {
            LOG.error("get hdfs namenode jvm metrics failed.", e);
            return null;
        }
    }

    public static JvmMetrics parseFromHBaseMaster(String json) {
        return parseFromHdfsNamenode(json);
    }

    public static JvmMetrics parseFromHBaseRegionserver(String json) {
        return parseFromHBaseMaster(json);
    }

    public Float getMemHeapUsedM() {
        return memHeapUsedM;
    }

    public void setMemHeapUsedM(Float memHeapUsedM) {
        this.memHeapUsedM = memHeapUsedM;
    }

    public Long getGcCount() {
        return gcCount;
    }

    public void setGcCount(Long gcCount) {
        this.gcCount = gcCount;
    }

    public Long getGcTimeMillis() {
        return gcTimeMillis;
    }

    public void setGcTimeMillis(Long gcTimeMillis) {
        this.gcTimeMillis = gcTimeMillis;
    }

    public Long getRunnableThreads() {
        return runnableThreads;
    }

    public void setRunnableThreads(Long runnableThreads) {
        this.runnableThreads = runnableThreads;
    }

    public Long getBlockedThreads() {
        return blockedThreads;
    }

    public void setBlockedThreads(Long blockedThreads) {
        this.blockedThreads = blockedThreads;
    }
}
