package com.yjhbase.etl.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Created by zhengzhubin on 2019/5/10.
 */
public class SparkRowUtil {


    private static int fieldIndex(Row r , String field){
        return r.fieldIndex(field);
    }

    public static boolean isNullCell(Row r, String field){
        return r.get(fieldIndex(r , field)) == null;
    }

    public static boolean isNullCell(Row r, int i){
        return r.get(i) == null;
    }

    public static String getStringCell(Row r, String field){
        return getStringCell(r , fieldIndex(r, field));
    }

    public static String getStringCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        String retValue = r.getString(i);
        if(retValue == null || retValue.trim().length() == 0) return null;
        return retValue;
    }

    public static Float getFloatCell(Row r, String field){
        return getFloatCell(r, fieldIndex(r , field));
    }

    public static Float getFloatCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        return r.getFloat(i);
    }

    public static Integer getIntCell(Row r, String field){
        return getIntCell(r , fieldIndex(r , field));
    }

    public static Integer getIntCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        return r.getInt(i);
    }

    public static Long getLongCell(Row r, String field){
        return getLongCell(r , fieldIndex(r, field));
    }

    public static Long getLongCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        return r.getLong(i);
    }

    public static Double getDoubleCell(Row r, String field){
        return getDoubleCell(r , fieldIndex(r , field));
    }

    public static Double getDoubleCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        return r.getDouble(i);
    }

    public static <T> List<T> getListCell(Row r, String field){
        return getListCell(r , fieldIndex(r , field));
    }

    public static <T> List<T> getListCell(Row r, int i){
        if(isNullCell(r , i)) return null;
        List<T> value = r.getList(i);
        return value;
    }

    public static void putIfNotNull(Map<String, Object> map, String key, Object value){
        if(value == null) return;

        if(value instanceof  String) {
            String valueStr = (String) value;
            if(StringUtils.isBlank(valueStr)) return;
            map.put(key, valueStr.trim());
            return;
        }
        if(value instanceof List){
            List listValue = (List) value;
            if(listValue.size() == 0) return;
        }
        if(value instanceof Map){
            Map mapValue = (Map) value;
            if(mapValue.size() == 0) return;
        }
        map.put(key , value);
    }
}
