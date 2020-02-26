package com.yjhbase.etl.jobs.imp;

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * @author zhengzhubin
 * @date 2020/2/24
 * @description
 **/
public abstract class AbstractImpJob implements Serializable {

    ImpJobOption jobOption;

    public AbstractImpJob(ImpJobOption jobOption){
        this.jobOption = jobOption;
        this.prepare();
    }

    public void prepare() {}

    public abstract void run() throws Exception;

    public ImpJobOption getJobOption() {
        return jobOption;
    }
}
