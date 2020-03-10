package com.yjhbase.etl.jobs.imp.functions;

import com.alibaba.fastjson.JSONObject;
import com.yjhbase.etl.dto.RkColumn;
import com.yjhbase.etl.dto.SparkColumn;
import com.yjhbase.etl.jobs.imp.HiveToHbaseJobOption;
import com.yjhbase.etl.utils.HBaseRowkeyUtil;
import com.yjhbase.etl.utils.SparkRowUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/3/9
 * @description
 **/
public class HiveToHBasePairFlatMapFunction implements PairFlatMapFunction<Row, ImmutableBytesWritable, KeyValue> {

    private static Logger LOG = LoggerFactory.getLogger(HiveToHBasePairFlatMapFunction.class);

    HiveToHbaseJobOption jobOption;

    List<SparkColumn> columnsList = null;

    byte[] cfBytes = null;

    public HiveToHBasePairFlatMapFunction() {}

    public HiveToHBasePairFlatMapFunction(HiveToHbaseJobOption jobOption) {
        this.jobOption = jobOption;
    }

    private synchronized void buildColumnsMap(Row row) {
        if(this.columnsList != null) return;

        this.columnsList = new ArrayList<>();
        this.cfBytes = Bytes.toBytes(jobOption.getHbaseColumnfamily());
        StructField[] fields = row.schema().fields();
        List<String> outFields = new ArrayList<>();
        for(StructField f : fields) {
            SparkColumn column = new SparkColumn();
            column.setName(f.name());
            column.setDataType(f.dataType());
            columnsList.add(column);
            outFields.add(column.getLowerCaseName());
        }
        columnsList.sort(new Comparator<SparkColumn>() {
            @Override
            public int compare(SparkColumn o1, SparkColumn o2) {
                return o1.getLowerCaseName().compareTo(o2.getLowerCaseName());
            }
        });
        for(RkColumn rkColumn : this.jobOption.getRkColumns()) {
            boolean flag = false;
            for(SparkColumn c : this.columnsList) {
                if(rkColumn.getLowerCaseName().equals(c.getLowerCaseName())) {
                    flag = true; break;
                }
            }
            if(!flag) {
                throw new IllegalArgumentException("not found key column " + rkColumn.getLowerCaseName() +
                        " in spark sql select result(out columns): " + JSONObject.toJSONString(outFields));
            }
        }
    }

    @Override
    public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(Row row) throws Exception {
        this.buildColumnsMap(row);
        Map<String, String> columnsDataMap = new HashMap<>();
        for(SparkColumn column : this.columnsList) {
            Object value = SparkRowUtil.getObjectCell(row, column.getName());
            String strValue = this.parse(column, value);
            if(strValue == null) continue;
            columnsDataMap.put(column.getLowerCaseName(), strValue);
        }
        String rkString = HBaseRowkeyUtil.getRowkey(columnsDataMap, this.jobOption.getRkColumns(), this.jobOption.getRkSplitKey());
        byte[] rkBytes = Bytes.toBytes(rkString);
        byte[] nextRkBytes = ClientUtil.calculateTheClosestNextRowKeyForPrefix(rkBytes);
        List<Tuple2<ImmutableBytesWritable, KeyValue>> retKvs =  new ArrayList<>();
        int cId = 0;
        for(SparkColumn c : this.columnsList) {
            if(!columnsDataMap.containsKey(c.getLowerCaseName())) continue;
            ImmutableBytesWritable keyWritable =
                    new ImmutableBytesWritable(bytesMerged(nextRkBytes, Bytes.toBytes(String.format("%03d", cId))));
            KeyValue retKv =
                    new KeyValue(rkBytes, this.cfBytes, Bytes.toBytes(c.getLowerCaseName()), Bytes.toBytes(columnsDataMap.get(c.getLowerCaseName())));
            retKvs.add(new Tuple2<>(keyWritable, retKv));
            cId ++;
        }
        return retKvs.iterator();
    }

    /**
     * 两个字节数组合并
     * @param b1
     * @param b2
     * @return
     */
    public static byte[] bytesMerged(byte[] b1, byte[] b2){
        int len = b1.length + b2.length;
        byte[] retBytes = new byte[len];
        int i = 0;
        for(byte b : b1) {
            retBytes[i] = b;
            i += 1;
        }
        for(byte b : b2) {
            retBytes[i] = b;
            i += 1;
        }
        return retBytes;
    }

    /**
     * 数据格式转换
     * @param column
     * @param value
     * @return
     */
    private String parse(SparkColumn column, Object value) {
        if(value == null) return null;
        if(column.getDataType() instanceof ArrayType ||
                column.getDataType() instanceof MapType) {
            return JSONObject.toJSONString(value);
        } else {
            return value + "";
        }
    }
}
