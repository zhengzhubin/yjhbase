package com.yjhbase.monitor.utils;

import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/4/1
 * @description
 **/
public class DataUtils {

    public static long parseToLong(String value) {
        return Long.parseLong(value);
    }

    public static long longValue(Map<String, Object> map, String key, long defaultValue) {
        if(!map.containsKey(key)) return defaultValue;
        return parseToLong(map.get(key) + "");
    }
}
