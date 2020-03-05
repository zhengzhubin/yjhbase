package com.yjhbase.monitor;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
@SpringBootApplication(scanBasePackages = {

})
@EnableScheduling
public class MonitorServerBootStrap implements CommandLineRunner {
    @Override
    public void run(String... strings) throws Exception {

    }

    public static void main(String[] args){
        SpringApplication.run(MonitorServerBootStrap.class , args);
    }
}
