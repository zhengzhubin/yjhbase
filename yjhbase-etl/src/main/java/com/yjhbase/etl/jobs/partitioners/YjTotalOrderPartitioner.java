//package com.yjhbase.etl.jobs.partitioners;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
//import org.apache.hadoop.util.ReflectionUtils;
//
//import java.io.Closeable;
//import java.io.IOException;
//import java.lang.reflect.Array;
//import java.util.ArrayList;
//
///**
// * Created by zhengzhubin on 2019/4/1.
// */
//public class YjTotalOrderPartitioner<K extends WritableComparable<?>, V> extends TotalOrderPartitioner<K , V> {
//
//    private static final Log LOG = LogFactory.getLog(YjTotalOrderPartitioner.class);
//    public static String YJ_MAPREDUCE_REDUCE_MULTIPLE = "yj.mapreduce.reduce.multiple";
//
//    private Configuration conf;
//    private Integer multiFactor = 1;
//    public YjTotalOrderPartitioner(){
//        super();
//    }
//
//    @Override
//    public Configuration getConf() {
//        return this.conf;
//    }
//
//    @Override
//    public int getPartition(K key, V value, int numPartitions) {
//        int partition = super.getPartition(key , value , numPartitions);
//        return partition*this.multiFactor + (key.hashCode() & Integer.MAX_VALUE)%this.multiFactor;
//    }
//
//}
