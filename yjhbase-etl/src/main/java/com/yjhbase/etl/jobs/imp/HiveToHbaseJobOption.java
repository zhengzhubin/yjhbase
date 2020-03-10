package com.yjhbase.etl.jobs.imp;

import com.yjhbase.etl.dto.RkColumn;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/2/25
 * @description
 **/
public class HiveToHbaseJobOption extends ImpJobOption {

    String sparkSql;

    String hbaseTablename;

    String hbaseColumnfamily;

    String outHBaseHdfsPath;

    List<RkColumn> rkColumns;

    String rkSplitKey = "::";

    Integer numberOfFilesPerRegion = 1;

    public String getHbaseTablename() {
        return hbaseTablename;
    }

    public void setHbaseTablename(String hbaseTablename) {
        this.hbaseTablename = hbaseTablename;
    }

    public String getOutHBaseHdfsPath() {
        return this.outHBaseHdfsPath;
    }

    public void setOutHBaseHdfsPath(String outHBaseHdfsPath) {
        this.outHBaseHdfsPath = outHBaseHdfsPath;
    }

    public String getSparkSql() {
        return sparkSql;
    }

    public void setSparkSql(String sparkSql) {
        this.sparkSql = sparkSql;
    }

    public String getHbaseColumnfamily() {
        return hbaseColumnfamily;
    }

    public void setHbaseColumnfamily(String hbaseColumnfamily) {
        this.hbaseColumnfamily = hbaseColumnfamily;
    }

    public List<RkColumn> getRkColumns() {
        return rkColumns;
    }

    public void setRkColumns(List<RkColumn> rkColumns) {
        this.rkColumns = rkColumns;
    }

    public String getRkSplitKey() {
        return rkSplitKey == null ? "::" : rkSplitKey;
    }

    public void setRkSplitKey(String rkSplitKey) {
        this.rkSplitKey = rkSplitKey;
    }

    public Integer getNumberOfFilesPerRegion() {
        return numberOfFilesPerRegion == null ? 1 : numberOfFilesPerRegion;
    }

    public void setNumberOfFilesPerRegion(Integer numberOfFilesPerRegion) {
        this.numberOfFilesPerRegion = numberOfFilesPerRegion;
    }
}
