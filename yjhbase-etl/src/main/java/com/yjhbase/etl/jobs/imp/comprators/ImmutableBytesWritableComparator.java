package com.yjhbase.etl.jobs.imp.comprators;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author zhengzhubin
 * @date 2020/2/28
 * @description
 **/
public class ImmutableBytesWritableComparator implements Serializable, Comparator<ImmutableBytesWritable> {
    @Override
    public int compare(ImmutableBytesWritable o1, ImmutableBytesWritable o2) {
        return o1.compareTo(o2);
    }
}
