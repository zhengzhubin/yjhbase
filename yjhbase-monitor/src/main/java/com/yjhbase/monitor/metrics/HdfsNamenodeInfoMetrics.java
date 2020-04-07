package com.yjhbase.monitor.metrics;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/30
 * @description
 **/
public class HdfsNamenodeInfoMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(HdfsNamenodeInfoMetrics.class);

    Boolean onSafeMode;

    //PercentRemaining
    Float percentOfDiskRemaining;

    Long totalFiles;

    Long totalBlocks;

    Long numberOfMissingBlocks;

    Long numberOfMissingBlocksWithReplicationFactorOne;

    Long numberOfLiveDataNodes;

    Long numberOfDeadDatanodes;

    private HdfsNamenodeInfoMetrics() { }

    public static HdfsNamenodeInfoMetrics parse(String json) {
        try {
            HashMap jsonMap = JSONObject.parseObject(json, HashMap.class);
            HashMap dataMap =
                    JSONObject.parseArray(
                            JSONObject.toJSONString(jsonMap.get("beans")),
                            HashMap.class
                    ).get(0);
            HdfsNamenodeInfoMetrics infoMetrics = new HdfsNamenodeInfoMetrics();
            infoMetrics.setOnSafeMode((dataMap.get("Safemode") + "").length() == 0 ? false : true);
            infoMetrics.setPercentOfDiskRemaining(Float.parseFloat(dataMap.get("PercentRemaining") + ""));
            infoMetrics.setTotalFiles(Long.parseLong(dataMap.get("TotalFiles") + ""));
            infoMetrics.setTotalBlocks(Long.parseLong(dataMap.get("TotalBlocks") + ""));
            infoMetrics.setNumberOfMissingBlocks(Long.parseLong(dataMap.get("NumberOfMissingBlocks") + ""));
            infoMetrics.setNumberOfMissingBlocksWithReplicationFactorOne(Long.parseLong(dataMap.get("NumberOfMissingBlocksWithReplicationFactorOne") + ""));
            HashMap liveNodes = JSONObject.parseObject(dataMap.get("LiveNodes") + "", HashMap.class);
            infoMetrics.setNumberOfLiveDataNodes(liveNodes.size() + 0L);
            HashMap deadNodes = JSONObject.parseObject(dataMap.get("DeadNodes") + "", HashMap.class);
            infoMetrics.setNumberOfDeadDatanodes(deadNodes.size() + 0L);
            return infoMetrics;
        } catch (Exception e) {
            LOG.error("get hdfs namenode nodeInfo metrics failed.", e);
            return null;
        }
    }

    public Boolean getOnSafeMode() {
        return onSafeMode;
    }

    public void setOnSafeMode(Boolean onSafeMode) {
        this.onSafeMode = onSafeMode;
    }

    public Float getPercentOfDiskRemaining() {
        return percentOfDiskRemaining;
    }

    public void setPercentOfDiskRemaining(Float percentOfDiskRemaining) {
        this.percentOfDiskRemaining = percentOfDiskRemaining;
    }

    public Long getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(Long totalFiles) {
        this.totalFiles = totalFiles;
    }

    public Long getTotalBlocks() {
        return totalBlocks;
    }

    public void setTotalBlocks(Long totalBlocks) {
        this.totalBlocks = totalBlocks;
    }

    public Long getNumberOfMissingBlocks() {
        return numberOfMissingBlocks;
    }

    public void setNumberOfMissingBlocks(Long numberOfMissingBlocks) {
        this.numberOfMissingBlocks = numberOfMissingBlocks;
    }

    public Long getNumberOfMissingBlocksWithReplicationFactorOne() {
        return numberOfMissingBlocksWithReplicationFactorOne;
    }

    public void setNumberOfMissingBlocksWithReplicationFactorOne(Long numberOfMissingBlocksWithReplicationFactorOne) {
        this.numberOfMissingBlocksWithReplicationFactorOne = numberOfMissingBlocksWithReplicationFactorOne;
    }

    public Long getNumberOfLiveDataNodes() {
        return numberOfLiveDataNodes;
    }

    public void setNumberOfLiveDataNodes(Long numberOfLiveDataNodes) {
        this.numberOfLiveDataNodes = numberOfLiveDataNodes;
    }

    public Long getNumberOfDeadDatanodes() {
        return numberOfDeadDatanodes;
    }

    public void setNumberOfDeadDatanodes(Long numberOfDeadDatanodes) {
        this.numberOfDeadDatanodes = numberOfDeadDatanodes;
    }
}
