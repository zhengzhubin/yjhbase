package com.yjhbase.etl.jobs.imp.functions;

import com.yjhbase.etl.utils.SparkRowUtil;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/2
 * @description
 **/
public class ConsumersFlatMapFunction implements Serializable, FlatMapFunction<Row, String> {

    @Override
    public Iterator<String> call(Row row) throws Exception {
        List<String> consumersId =  new ArrayList<>();
        consumersId.add(SparkRowUtil.getStringCell(row, "consumerId"));
        return consumersId.iterator();
    }
}
