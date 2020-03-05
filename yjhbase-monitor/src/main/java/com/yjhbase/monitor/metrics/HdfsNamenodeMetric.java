package com.yjhbase.monitor.metrics;

import com.yjhbase.monitor.common.StatusEnum;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public class HdfsNamenodeMetric extends BaseMetric{

    StatusEnum status = StatusEnum.UNKNOWN;

    Boolean onSaleMode;

    //堆内存使用情况
    Float memHeapUsedM;

    // gc次数
    Integer gcCount;

    //gc 累计耗时
    Integer gcTimeMillis;

    Float diskRemainingPercent;

    Integer totalFiles;

    Integer totalBlocks;

    Integer numberOfMissingBlocks;

    Integer numberOfMissingBlocksWithReplicationFactorOne;

    Integer numberOfDeadDatanodes;

    public HdfsNamenodeMetric(String ip, Integer port) {
        super(ip, port);
    }


    public StatusEnum getStatus() {
        return status;
    }

    public void setStatus(StatusEnum status) {
        this.status = status;
    }

    public Boolean getOnSaleMode() {
        return onSaleMode;
    }

    public void setOnSaleMode(Boolean onSaleMode) {
        this.onSaleMode = onSaleMode;
    }

    public Float getMemHeapUsedM() {
        return memHeapUsedM;
    }

    public void setMemHeapUsedM(Float memHeapUsedM) {
        this.memHeapUsedM = memHeapUsedM;
    }

    public Integer getGcCount() {
        return gcCount;
    }

    public void setGcCount(Integer gcCount) {
        this.gcCount = gcCount;
    }

    public Integer getGcTimeMillis() {
        return gcTimeMillis;
    }

    public void setGcTimeMillis(Integer gcTimeMillis) {
        this.gcTimeMillis = gcTimeMillis;
    }

    public Float getDiskRemainingPercent() {
        return diskRemainingPercent;
    }

    public void setDiskRemainingPercent(Float diskRemainingPercent) {
        this.diskRemainingPercent = diskRemainingPercent;
    }

    public Integer getTotalFiles() {
        return totalFiles;
    }

    public void setTotalFiles(Integer totalFiles) {
        this.totalFiles = totalFiles;
    }

    public Integer getTotalBlocks() {
        return totalBlocks;
    }

    public void setTotalBlocks(Integer totalBlocks) {
        this.totalBlocks = totalBlocks;
    }

    public Integer getNumberOfMissingBlocks() {
        return numberOfMissingBlocks;
    }

    public void setNumberOfMissingBlocks(Integer numberOfMissingBlocks) {
        this.numberOfMissingBlocks = numberOfMissingBlocks;
    }

    public Integer getNumberOfMissingBlocksWithReplicationFactorOne() {
        return numberOfMissingBlocksWithReplicationFactorOne;
    }

    public void setNumberOfMissingBlocksWithReplicationFactorOne(Integer numberOfMissingBlocksWithReplicationFactorOne) {
        this.numberOfMissingBlocksWithReplicationFactorOne = numberOfMissingBlocksWithReplicationFactorOne;
    }

    public Integer getNumberOfDeadDatanodes() {
        return numberOfDeadDatanodes;
    }

    public void setNumberOfDeadDatanodes(Integer numberOfDeadDatanodes) {
        this.numberOfDeadDatanodes = numberOfDeadDatanodes;
    }
}
