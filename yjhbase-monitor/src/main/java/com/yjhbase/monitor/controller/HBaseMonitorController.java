package com.yjhbase.monitor.controller;

import com.yjhbase.monitor.hbase.service.HBaseMasterMonitorService;
import com.yjhbase.monitor.hbase.service.HBaseRsMonitorService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author zhengzhubin
 * @date 2020/4/3
 * @description
 **/
@Controller
@RequestMapping(value = "/yjmonitor/hbase")
@Api(value = "/yjmonitor/hbase", description = "HBase 监控接口")
public class HBaseMonitorController {

    private final Logger LOG = LoggerFactory.getLogger(HBaseMonitorController.class);

    @Resource
    HBaseMasterMonitorService masterMonitorService;

    @Resource
    HBaseRsMonitorService rsMonitorService;


    @ApiOperation(value = "master 相关监控信息")
    @RequestMapping(value = "/master" , method = RequestMethod.GET)
    @ResponseBody
    public void getMastersStatus(HttpServletResponse resp) throws Exception {
        this.masterMonitorService.buildHBaseMasterMonitorInfoWithPrometheusFormat(resp.getWriter());
    }

    @ApiOperation(value = "regionserver 相关监控信息")
    @RequestMapping(value = "/regionserver" , method = RequestMethod.GET)
    @ResponseBody
    public void getRegionserversStatus(HttpServletResponse resp) throws Exception {
        this.rsMonitorService.buildHBaseRsMonitorInfoWithPrometheusFormat(resp.getWriter());
    }


}
