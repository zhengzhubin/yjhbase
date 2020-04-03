package com.yjhbase.monitor.controller;

import com.yjhbase.monitor.hdfs.service.HdfsNamenodeMonitorService;
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
@RequestMapping(value = "/yjmonitor/hdfs")
@Api(value = "/yjmonitor/hdfs", description = "hdfs 监控接口")
public class HdfsMonitorController {

    @Resource
    HdfsNamenodeMonitorService nnMonitorService;

    @ApiOperation(value = "namenode 相关监控信息")
    @RequestMapping(value = "/namenode" , method = RequestMethod.GET)
    @ResponseBody
    public void getMastersStatus(HttpServletResponse resp) throws Exception {
        this.nnMonitorService.buildHdfsnamenodeMonitorInfoWithPrometheusFormat(resp.getWriter());
    }


}
