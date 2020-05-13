package com.yjhbase.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author zhengzhubin
 * @date 2020/4/23
 * @description
 **/

@SpringBootApplication(scanBasePackages = {
        "com.yjhbase.manager.common",
        "com.yjhbase.manager.service",
        "com.yjhbase.manager.controller"
})
@EnableScheduling
@EnableSwagger2
public class ServerBootStrap implements CommandLineRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ServerBootStrap.class);

    @Override
    public void run(String... strings) throws Exception {
        LOG.info("yjhbase manager plat server start ...... ");
    }

    public static void main(String[] args){
        SpringApplication.run(ServerBootStrap.class , args);
    }
}
