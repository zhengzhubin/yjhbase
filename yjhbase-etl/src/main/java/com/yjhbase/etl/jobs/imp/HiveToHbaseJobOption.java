package com.yjhbase.etl.jobs.imp;

import java.io.Serializable;

/**
 * @author zhengzhubin
 * @date 2020/2/25
 * @description
 **/
public class HiveToHbaseJobOption extends ImpJobOption {

    String hiveTablename;

    String hbaseTablename;

    String outHBaseHdfsPath;

    public String getHiveTablename() {
        return hiveTablename;
    }

    public void setHiveTablename(String hiveTablename) {
        this.hiveTablename = hiveTablename;
    }

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
}
