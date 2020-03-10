package com.yjhbase.etl.dto;

/**
 * @author zhengzhubin
 * @date 2020/3/9
 * @description
 **/
public class RkColumn extends SparkColumn {

    Integer priority;

    Boolean useHash;

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public void hashkeyOrNot(Integer value) {
        this.useHash = (value == 1 ? true : false);
    }

    public Boolean getUseHash() {
        return useHash;
    }

    public void setUseHash(Boolean useHash) {
        this.useHash = useHash;
    }
}
