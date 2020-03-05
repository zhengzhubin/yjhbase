package com.yjhbase.monitor.zookeeper.service;

import com.yjhbase.monitor.common.ServerConfig;
import com.yjhbase.monitor.common.StatusEnum;
import com.yjhbase.monitor.metrics.BaseMetric;
import com.yjhbase.monitor.metrics.ZkStateMetric;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@Service
public class ZookeeperMonitorServiceImpl implements ZookeeperMonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperMonitorServiceImpl.class);

    @Autowired
    ServerConfig serverConfig;

    List<ServerConfig.ServerNode> zkNodes = null;
    List<ZkStateMetric> zkNodesStateMetric = new ArrayList<>();

    @PostConstruct
    public void prepare() {
        this.zkNodes = this.serverConfig.getZkNodes();
    }

    @Override
    public List<ZkStateMetric> getZkNodesStateMetric() {
        synchronized (this.zkNodes) {
            return BaseMetric.copy(this.zkNodesStateMetric);
        }
    }

    @Scheduled(cron = "*/60 * * * * ?")
    public void run(){
        synchronized (this.zkNodes) {
            this.zkNodesStateMetric = new ArrayList<>();
            for (ServerConfig.ServerNode node : this.zkNodes) {
                this.zkNodesStateMetric.add(this.getzkNodeState(node));
            }
        }
    }

    private ZkStateMetric getzkNodeState(ServerConfig.ServerNode node) {
        ZkStateMetric stateMetric = new ZkStateMetric(node.getIp(), node.getPort());
        String statInfo = null;
        try{
            statInfo = FourLetterWordMain.send4LetterWord(node.getIp() , node.getPort() , "stat");
        }catch (Exception e) {
            LOG.error("监控zookeeper节点信息失败, ip = " + node.getIp(), e);
            stateMetric.setStatus(StatusEnum.UNKNOWN);
            return stateMetric;
        }
        if(statInfo == null) {
            stateMetric.setStatus(StatusEnum.OFFLINE);
            return stateMetric;
        } else {
            stateMetric.setStatus(StatusEnum.ONLINE);
            try {
                String[] lines = statInfo.split("\n");
                for(String line: lines) {
                    if(line.trim().startsWith("Connections: ")) {
                        stateMetric.setConnections(Integer.parseInt(line.replace("Connections: " , "").trim()));
                    } else if(line.trim().startsWith("Latency ")) {
                        String[] latency = line.replace("Latency ","").split(":")[1].split("/");
                        stateMetric.setAvgLatency(Integer.parseInt(latency[1].trim()));
                        stateMetric.setMaxLatency(Integer.parseInt(latency[2].trim()));
                    }
                }
            } catch (Exception e) {
                LOG.error("zookeeper stat 信息解析失败,msg: " + statInfo, e);
            }
        }
        return stateMetric;
    }
}
