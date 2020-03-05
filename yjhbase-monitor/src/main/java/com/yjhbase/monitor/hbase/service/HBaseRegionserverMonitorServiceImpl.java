package com.yjhbase.monitor.hbase.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.metrics.HBaseRegionserverMetric;
import com.yjhbase.monitor.metrics.HBaseTableMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public class HBaseRegionserverMonitorServiceImpl implements HBaseRegionserverMonitorService{

    private static final Logger LOG = LoggerFactory.getLogger(HBaseRegionserverMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> regionservers = null;

    @PostConstruct
    public void prepare() {
        this.regionservers = this.serverConfig.getHBaseRegionservers();
    }

    @Override
    public List<HBaseRegionserverMetric> getHBaseRegionserversMetric() {
        return null;
    }

    @Override
    public List<HBaseTableMetric> getHBaseTablesMetric() {
        return null;
    }


}
