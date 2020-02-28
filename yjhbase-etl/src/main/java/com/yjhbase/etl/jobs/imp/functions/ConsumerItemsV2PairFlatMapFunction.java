package com.yjhbase.etl.jobs.imp.functions;

import com.alibaba.fastjson.JSONObject;
import com.yjhbase.etl.utils.SparkRowUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/2/27
 * @description
 **/
public class ConsumerItemsV2PairFlatMapFunction
        implements PairFlatMapFunction<Row, ImmutableBytesWritable, Put> {

    int itemsPerPage = 100;
    Random random = new Random();

    @Override
    public Iterator<Tuple2<ImmutableBytesWritable, Put>> call(Row row) throws Exception {
        String consumerId = SparkRowUtil.getStringCell(row, "consumerId");

        List<Integer> items = this.itemsList();
        int len = items.size();
        int pages = (len - 1) / itemsPerPage + 1;
        List<Tuple2<ImmutableBytesWritable, Put>> retPuts = new ArrayList<>();
        for(int i = 0; i<= pages; i ++) {
            String rkString = rowkey(consumerId, i);
            List<Integer> valueList = this.pageItems(items, len, i);
            byte[] rkBytes = Bytes.toBytes(rkString);
            ImmutableBytesWritable rkWritable = new ImmutableBytesWritable(rkBytes);
            Put put = new Put(rkBytes);
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("consumerId"), Bytes.toBytes(consumerId));
            put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("itemsId"), Bytes.toBytes(JSONObject.toJSONString(valueList)));
            if(i == 1) {
                put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("pages"), Bytes.toBytes(pages + ""));
            }
            retPuts.add(new Tuple2<>(rkWritable, put));

//            retKVs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + "::f:consumerId")),
//                    new KeyValue(rkBytes, )));
//            retKVs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + "::f:itemsId")),
//                    new KeyValue(rkBytes, )));
//            if(i == 1) {
//                //在第一页记录总页数
//                retKVs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + "::f:pages")),
//                        new KeyValue(rkBytes,
//                                Bytes.toBytes("f"), Bytes.toBytes("pages"), Bytes.toBytes(pages + "")))
//                );
//            }
        }
        return retPuts.iterator();
    }


    private List<Integer> pageItems(List<Integer> items, int len, int pageId) {
        if(pageId == 0) return items;
        return items.subList((pageId - 1)* itemsPerPage, Math.min(len, pageId * itemsPerPage));
    }

    private List<Integer> itemsList() {
//        200:0~15 500:16~23 1000:24~27 3000: 28~29 10000:30
        int p = Math.abs(random.nextInt(31)) % 31;
        int len = p < 16 ? 200 : (p < 24 ? 500 : (p < 28 ? 1000 : (p < 30 ? 3000 : 10000)));
        List<Integer> retItems = new ArrayList<>();
        for(int i = 0; i < len; i ++) {
            retItems.add(500000 + this.random.nextInt(500000));
        }
        return retItems;
    }

    private static String rowkey(String value, int pageId) {
        int code = (value.hashCode() & Integer.MAX_VALUE) % 1000;
        return String.format("%03d::%s", code, value) + "::" + pageId;
    }
}
