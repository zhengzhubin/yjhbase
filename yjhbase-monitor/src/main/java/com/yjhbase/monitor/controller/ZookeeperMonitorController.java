package com.yjhbase.monitor.controller;

import com.yjhbase.monitor.zookeeper.service.ZookeeperMonitorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;

/**
 * @author zhengzhubin
 * @date 2020/4/3
 * @description
 **/
@Controller
@RequestMapping(value = "/yjmonitor/zookeeper")
@Api(value = "/yjmonitor/zookeeper", description = "zookeeper 监控接口")
public class ZookeeperMonitorController {

    @Resource
    ZookeeperMonitorService zkMonitorService;

    @ApiOperation(value = "zookeeper 相关监控信息")
    @RequestMapping(value = "/zookeeper" , method = RequestMethod.GET)
    @ResponseBody
    public void getMastersStatus(HttpServletResponse resp) throws Exception {
        this.zkMonitorService.buildZookeepersMonitorInfoWithPrometheusFormat(resp.getWriter());
    }



}
