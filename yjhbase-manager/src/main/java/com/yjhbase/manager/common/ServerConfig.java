package com.yjhbase.manager.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author zhengzhubin
 * @date 2020/4/26
 * @description
 **/
@Component
public class ServerConfig {

    @Value("${yjhbase.zookeeper.nodes}")
    String zkNodes;

    @Value("${yjhbase.zookeeper.port}")
    Integer zkPort;

    @Value("${yjhbase.zookeeper.parent}")
    String zkParent;

    public String getZkNodes() {
        return zkNodes;
    }

    public Integer getZkPort() {
        return zkPort;
    }

    public String getZkParent() {
        return zkParent;
    }
}
