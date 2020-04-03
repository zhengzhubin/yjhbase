package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.utils.OkHttpUtil;
import okhttp3.Response;

import java.io.IOException;
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

    protected String httpRequest(String url) throws Exception {
        Response resp = OkHttpUtil.httpGet(url);
        if(resp.code() != 200) {
            resp.message();
            throw new IOException("http request failed, url: " + url + " \n " + resp.message());
        }
        String retString = resp.body().string();
        try{
            resp.close();
        }catch (Exception e) {}
        return retString;
    }
}
