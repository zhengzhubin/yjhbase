package org.apache.hadoop.hbase.quotas;

/**
 * @author zhengzhubin
 * @date 2020/1/17
 * @description threw requeueException when exceed quota limit
 **/
public class QuotaRequeueException extends RpcThrottlingException{

    public QuotaRequeueException(String msg) {
        super(msg);
    }
}
