package com.yjhbase.etl.dto;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;

import java.io.Serializable;

/**
 * @author zhengzhubin
 * @date 2020/3/9
 * @description
 **/
public class SparkColumn implements Serializable {

    String name;

    String lowerCaseName;

    DataType dataType;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
        this.lowerCaseName = this.name.toLowerCase();
    }

    public String getLowerCaseName() {
        return lowerCaseName;
    }

    public void setLowerCaseName(String lowerCaseName) {
        this.lowerCaseName = lowerCaseName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }
}
