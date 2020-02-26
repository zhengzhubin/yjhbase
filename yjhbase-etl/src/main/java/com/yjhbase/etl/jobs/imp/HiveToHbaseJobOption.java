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

    String hbaseZookeeper;

    String hbaseZnode;

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

    public String getHbaseZookeeper() {
        return hbaseZookeeper;
    }

    public void setHbaseZookeeper(String hbaseZookeeper) {
        this.hbaseZookeeper = hbaseZookeeper;
    }

    public String getHbaseZnode() {
        return hbaseZnode;
    }

    public void setHbaseZnode(String hbaseZnode) {
        this.hbaseZnode = hbaseZnode;
    }
}
