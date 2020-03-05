package com.yjhbase.monitor.metrics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public abstract class BaseMetric implements Serializable {

    String ip;

    Integer port;

    public BaseMetric(String ip, Integer port) {
        this.ip = ip; this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public static <T extends BaseMetric> List<T> copy(List<T> list) {
        if(list == null) return null;
        List<T> retList = new ArrayList<>();
        for(T o : list) retList.add(o);
        return retList;
    }
}
