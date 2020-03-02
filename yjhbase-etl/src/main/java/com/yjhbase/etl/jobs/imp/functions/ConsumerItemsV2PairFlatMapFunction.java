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
        implements PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue> {

    int itemsPerPage = 100;
    Random random = new Random();

    @Override
    public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
        String consumerId = SparkRowUtil.getStringCell(row, "consumerId");

        List<Integer> items = this.itemsList();
        int len = items.size();
        int pages = (len - 1) / itemsPerPage + 1;
        List<Tuple2<ImmutableBytesWritable, KeyValue>> retKvs = new ArrayList<>();
        for(int i = 0; i<= pages; i ++) {
            String rkString = rowkey(consumerId, i);
            List<Integer> valueList = this.pageItems(items, len, i);
            byte[] rkBytes = Bytes.toBytes(rkString);
//            ImmutableBytesWritable rkWritable = new ImmutableBytesWritable(rkBytes);
            retKvs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + " f:c")),
                    new KeyValue(rkBytes, Bytes.toBytes("f"), Bytes.toBytes("consumerId"),
                            Bytes.toBytes(consumerId))));
            String itemsId = JSONObject.toJSONString(valueList);
            retKvs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + " f:i")),
                    new KeyValue(rkBytes, Bytes.toBytes("f"), Bytes.toBytes("itemsId"),
                            Bytes.toBytes(itemsId))));
            if(i == 1) {
                retKvs.add(new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(rkString + " f:p")),
                        new KeyValue(rkBytes, Bytes.toBytes("f"), Bytes.toBytes("pages"),
                                Bytes.toBytes(pages + ""))));
            }
        }
        return retKvs.iterator();
    }


    private List<Integer> pageItems(List<Integer> items, int len, int pageId) {
        if(pageId == 0) return items;
        return items.subList((pageId - 1)* itemsPerPage, Math.min(len, pageId * itemsPerPage));
    }

    private List<Integer> itemsList() {
//        500:0~15 1000:16~23 2000:24~27 3000: 28~29 10000:30
        int p = Math.abs(random.nextInt(5)) % 5;
        int len = p < 1 ? 500 : (p < 2 ? 1000 : (p < 3 ? 2000 : (p < 4 ? 3000 : 9000)));
        List<Integer> retItems = new ArrayList<>();
        for(int i = 0; i < len; i ++) {
            retItems.add(500000 + this.random.nextInt(500000));
        }
        return retItems;
    }

    private static String rowkey(String value, int pageId) {
        int code = (value.hashCode() & Integer.MAX_VALUE) % 1000;
        return String.format("%03d::%s::%02d", code, value, pageId);
    }
}
