package com.yjhbase.etl.utils;

import com.yjhbase.etl.dto.RkColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/3/10
 * @description
 **/
public class HBaseRowkeyUtil {


    public static String getRowkey(Map<String, String> columnsDataMap, List<RkColumn> rkColumns, String splitKey) {
        String hashString = null;
        String retString = null;
        for(RkColumn rkColumn : rkColumns) {
            String columnValue =
                    (columnsDataMap.containsKey(rkColumn.getLowerCaseName()) ? columnsDataMap.get(rkColumn.getLowerCaseName()) : "null");
            if(retString == null) {
                retString = columnValue;
            } else {
                retString = retString + splitKey + columnValue;
            }
            if(!rkColumn.getUseHash()) continue;
            if(hashString == null) {
                hashString = columnValue;
            } else {
                hashString = hashString + splitKey + columnValue;
            }
        }
        if(hashString == null) return retString;
        int code = (hashString.hashCode() & Integer.MAX_VALUE) % 1000;
        return String.format("%03d%s%s", code, splitKey, retString);
    }

    public static void main(String[] args) {
        Map<String, String> columnsDataMap = new HashMap<>();
        columnsDataMap.put("tab_id", "102");
        columnsDataMap.put("consumer_id", "20129474");

        List<RkColumn> rkColumns = new ArrayList<>();
        RkColumn k1 = new RkColumn();
        k1.setName("tab_id");
        k1.setUseHash(true);
        RkColumn k2 = new RkColumn();
        k2.setName("consumer_id");
        k2.setUseHash(true);
        rkColumns.add(k1);
        rkColumns.add(k2);

        System.out.println(getRowkey(columnsDataMap, rkColumns, "::"));
    }
}
