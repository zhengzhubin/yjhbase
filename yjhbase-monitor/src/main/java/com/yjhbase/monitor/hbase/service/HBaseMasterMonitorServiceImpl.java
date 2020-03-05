package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.metrics.HBaseMasterMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public class HBaseMasterMonitorServiceImpl implements HBaseMasterMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseMasterMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> masters = null;

    @PostConstruct
    public void prepare() {
        this.masters = this.serverConfig.getHBaseMasters();
    }

    @Override
    public List<HBaseMasterMetric> getHBaseMastersMetric() {
        return null;
    }

    @Scheduled(cron = "*/60 * * * * ?")
    public void run(){

    }


}
