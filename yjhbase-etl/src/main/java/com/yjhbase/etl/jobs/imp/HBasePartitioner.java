package com.yjhbase.etl.jobs.imp;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.Partitioner;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/2/26
 * @description
 **/
public class HBasePartitioner extends Partitioner {

    Integer numRegions, hfilesPerRegion, totalHFiles;
    List<Pair<byte[], byte[]>> regions = new ArrayList<>();
    static Integer DEFAULT_hfilesPerRegionMax = 5;

    public HBasePartitioner(byte[][] startKeys){
        this(startKeys, 1);
    }

    public HBasePartitioner(byte[][] startKeys, int hfilesPerRegion){
        this.hfilesPerRegion = hfilesPerRegion > 1 ?
                Math.min(hfilesPerRegion, DEFAULT_hfilesPerRegionMax) : 1;
        if(startKeys == null || startKeys.length == 0) {
            this.numRegions = 1;
            this.regions.add(new Pair<>(null, null));
        } else {
            this.numRegions = startKeys.length;
            for(int i = 0; i < startKeys.length - 1; i ++) {
                this.regions.add(new Pair<>(startKeys[i].length == 0 ? null : startKeys[i], startKeys[i + 1]));
            }
            this.regions.add(new Pair<>(startKeys[startKeys.length - 1].length == 0 ? null : startKeys[startKeys.length - 1], null));
        }
        this.totalHFiles = this.numRegions * this.hfilesPerRegion;
    }

    @Override
    public int numPartitions() {
        return this.totalHFiles;
    }

    @Override
    public int getPartition(Object key) {
        int regionId = this.getRegion((ImmutableBytesWritable) key);
        return regionId * this.hfilesPerRegion + (key.hashCode() & Integer.MAX_VALUE) % this.hfilesPerRegion;
    }

    public int getRegion(ImmutableBytesWritable writable) {
        byte[] rkBytes = writable.get();
        int i = 0, j = this.numRegions - 1;
        while(i <= j) {
            int p = (i + j) / 2;
            int result = in(regions.get(p).getFirst(), regions.get(p).getSecond(), rkBytes);
            if(result == 0) return p;
            if(result < 0) {
                j = p -1;
            } else {
                i = p + 1;
            }
        }
        throw new RuntimeException("find rowkey's partition failed: rowkey = " + Bytes.toString(rkBytes));
    }

    private int in(byte[] startKey, byte[] endKey, byte[] rkBytes) {
        if(startKey == null && endKey == null) return 0;
        if(startKey == null) {
            int result = Bytes.compareTo(rkBytes, endKey);
            return result >= 0 ? 1 : 0;
        } else if(endKey == null) {
            int result = Bytes.compareTo(rkBytes, startKey);
            return result >= 0 ? 0 : 1;
        } else {
            if(Bytes.compareTo(rkBytes, endKey) >= 0) return 1;
            if(Bytes.compareTo(rkBytes, startKey) >= 0) return 0;
            return -1;
        }
    }
}
