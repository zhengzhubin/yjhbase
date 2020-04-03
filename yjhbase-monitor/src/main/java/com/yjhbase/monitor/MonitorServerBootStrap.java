package com.yjhbase.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@SpringBootApplication(scanBasePackages = {
        "com.yjhbase.monitor.common",
        "com.yjhbase.monitor.zookeeper.service",
        "com.yjhbase.monitor.hdfs.service",
        "com.yjhbase.monitor.hbase.service",
        "com.yjhbase.monitor.controller"
})
@EnableScheduling
@EnableSwagger2
public class MonitorServerBootStrap implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorServerBootStrap.class);

    @Override
    public void run(String... strings) throws Exception {
        LOG.info("yjhbase monitor server start ...... ");
    }

    public static void main(String[] args){
        SpringApplication.run(MonitorServerBootStrap.class , args);
    }
}
