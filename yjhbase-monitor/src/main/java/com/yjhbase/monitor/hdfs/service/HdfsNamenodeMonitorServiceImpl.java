package com.yjhbase.monitor.hdfs.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.common.StatusEnum;
import com.yjhbase.monitor.metrics.BaseMetric;
import com.yjhbase.monitor.metrics.HdfsNamenodeMetric;
import com.yjhbase.monitor.utils.OkHttpUtil;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@Service
public class HdfsNamenodeMonitorServiceImpl implements HdfsNamenodeMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsNamenodeMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> namenodes = null;

    List<HdfsNamenodeMetric> oldNodesMetric = new ArrayList<>();
    List<HdfsNamenodeMetric> namenodesMetric = new ArrayList<>();

    @PostConstruct
    public void prepare() {
        this.namenodes = this.serverConfig.getHdfsnamenodes();
    }

    @Override
    public List<HdfsNamenodeMetric> getHdfsNamenodesMetric() {
        synchronized (this.namenodes) {
            return BaseMetric.copy(this.namenodesMetric);
        }
    }

    @Scheduled(cron = "*/60 * * * * ?")
    public void run(){
        synchronized (this.namenodes) {
            this.oldNodesMetric = BaseMetric.copy(this.namenodesMetric);
            this.namenodesMetric = new ArrayList<>();
            for(ServerConfig.ServerNode node: this.namenodes) {
                HdfsNamenodeMetric metric = this.getNamenodeStatusMetric(node);
                try {
                    this.fillNamenodeJmxMetric(node, metric);
                } catch (Exception e) { }
                try {
                    this.fillNamenodeInfoMetric(node, metric);
                } catch (Exception e) { }
                this.namenodesMetric.add(metric);
            }
        }
    }

    /**
     * namenode 节点状态
     * @param node
     * @return
     */
    private HdfsNamenodeMetric getNamenodeStatusMetric(ServerConfig.ServerNode node) {
        HdfsNamenodeMetric retMetric  =new HdfsNamenodeMetric(node.getIp(), node.getPort());
        String url =  this.getNamenodeStatusMetricRequestUrl(node);
        try {
            String result = this.httpRequest(url);
            retMetric.setStatus(StatusEnum.ONLINE);
        }catch (Exception e) {
            retMetric.setStatus(StatusEnum.OFFLINE);
        }
        return retMetric;
    }

    private void fillNamenodeJmxMetric(ServerConfig.ServerNode node,
                                       HdfsNamenodeMetric metric) throws Exception{
        if(!metric.getStatus().equalTo(StatusEnum.ONLINE)) return;

        String url = this.getNamenodeJmxMetricRequestUrl(node);


    }


    private void fillNamenodeInfoMetric(ServerConfig.ServerNode node,
                                        HdfsNamenodeMetric metric) throws Exception{
        if(!metric.getStatus().equalTo(StatusEnum.ONLINE)) return;
    }

    private String httpRequest(String url) throws Exception {
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

    private String getNamenodeStatusMetricRequestUrl(ServerConfig.ServerNode node){
        return this.getBaseRequestUrl(node) + "Hadoop:service=NameNode,name=NameNodeStatus";
    }

    private String getNamenodeJmxMetricRequestUrl(ServerConfig.ServerNode node) {
        return this.getBaseRequestUrl(node) + "Hadoop:service=NameNode,name=JvmMetrics";
    }

    private String getNamenodeInfoMetricRequestUrl(ServerConfig.ServerNode node) {
        return this.getBaseRequestUrl(node) + "Hadoop:service=NameNode,name=NameNodeInfo";
    }

    private String getBaseRequestUrl(ServerConfig.ServerNode node) {
        return "http://" + node.getIp() + ":" + node.getPort() + "/jmx?qry=";
    }

}
