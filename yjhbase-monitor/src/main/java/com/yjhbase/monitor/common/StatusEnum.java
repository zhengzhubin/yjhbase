package com.yjhbase.monitor.common;

/**
 * @author zhengzhubin
 * @date 2020/3/4
 * @description
 **/
public enum StatusEnum {

    UNKNOWN (99, "未知"),
    ONLINE (0, "online"),
    OFFLINE(1, "offline");

    Integer statusId;

    String statusName;

    StatusEnum(Integer statusId, String statusName) {
        this.statusId = statusId;
        this.statusName = statusName;
    }

    public Integer getStatusId() {
        return statusId;
    }

    public String getStatusName() {
        return statusName;
    }

    public boolean equalTo(StatusEnum other) {
        if(other == null) return false;
        return this.getStatusId().intValue() == other.getStatusId().intValue();
    }
}