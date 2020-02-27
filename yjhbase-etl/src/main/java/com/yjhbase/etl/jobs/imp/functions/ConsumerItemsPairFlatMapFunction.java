package com.yjhbase.etl.jobs.imp.functions;

import com.alibaba.fastjson.JSONObject;
import com.yjhbase.etl.utils.SparkRowUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/2/27
 * @description
 **/
public class ConsumerItemsPairFlatMapFunction
        implements PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue>
{

    @Override
    public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
        String consumerId = SparkRowUtil.getStringCell(row, "consumerId");
        if(consumerId == null) return new ArrayList().iterator();
        List<String> itemsId = SparkRowUtil.getListCell(row, "itemsId");
        if(itemsId == null) return new ArrayList().iterator();
        byte[] rkBytes = Bytes.toBytes(this.rowkey(consumerId));
        ImmutableBytesWritable writable = new ImmutableBytesWritable(rkBytes);
        List<Tuple2<ImmutableBytesWritable, KeyValue>> retKVs = new ArrayList<>();
        retKVs.add(new Tuple2<>(
                writable,
                new KeyValue(rkBytes,
                        Bytes.toBytes("f"),
                        Bytes.toBytes("consumerId"),
                        Bytes.toBytes(consumerId)))
        );
        retKVs.add(new Tuple2<>(
                writable,
                new KeyValue(rkBytes,
                        Bytes.toBytes("f"),
                        Bytes.toBytes("itemsId"),
                        Bytes.toBytes(JSONObject.toJSONString(itemsId))))
        );
        return retKVs.iterator();
    }

    private static String rowkey(String value) {
        int code = (value.hashCode() & Integer.MAX_VALUE) % 1000;
        return String.format("%03d::%s", code, value);
    }
}
