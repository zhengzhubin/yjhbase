package com.yjhbase.etl.jobs.imp.functions;

import com.yjhbase.etl.utils.SparkRowUtil;
import org.apache.commons.lang3.StringUtils;
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
        String tab_id = getString(row, "tab_id");;
        String consumer_id = getString(row, "consumer_id");
        consumersId.add(getRowkey(tab_id, consumer_id));
        return consumersId.iterator();
    }

    private String getRowkey(String tabId, String consumerId) {
        String hashString = String.format("%s::%s", tabId, consumerId);
        int code = (hashString.hashCode() & Integer.MAX_VALUE) % 1000;
        return String.format("%03d::%s", code, hashString);
    }

    private String getString(Row r, String col) {
        Object objValue = SparkRowUtil.getObjectCell(r, col);
        return objValue == null ? "null" : (objValue + "");
    }

}
