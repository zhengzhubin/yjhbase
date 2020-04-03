package com.yjhbase.monitor.metrics;

import com.alibaba.fastjson.JSONObject;
import com.yjhbase.monitor.common.StatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author zhengzhubin
 * @date 2020/3/5
 * @description
 **/
public class HBaseRsMetric extends BaseMetric {

    private static Logger LOG = LoggerFactory.getLogger(HBaseRsMetric.class);

    StatusEnum status = StatusEnum.UNKNOWN;

    JvmMetrics jvmMetrics;

    HBaseRsIpcMetrics ipcMetrics;

    Map<String, HBaseRsTableMetrics> tablesMetrics = new HashMap<>();

    private HBaseRsMetric(String ip, Integer port) {
        super(ip, port);
    }

    public static HBaseRsMetric parse(String ip, Integer port){
        HBaseRsMetric rsMetric = new HBaseRsMetric(ip, port);
        return rsMetric.parse();
    }

    private HBaseRsMetric parse() {
        try {
            String jvmMetricsJson = this.httpRequest(
                    this.getBaseRequestUrl() + "Hadoop:service=HBase,name=JvmMetrics");
            this.jvmMetrics = JvmMetrics.parseFromHBaseRegionserver(jvmMetricsJson);

            String rsMetricsInfoJson = this.httpRequest(
                    this.getBaseRequestUrl() + "Hadoop:service=HBase,name=RegionServer,sub=*");
            this.ipcMetrics = HBaseRsIpcMetrics.parse(rsMetricsInfoJson);

            Map<String, Object> regionsDataMap =
                    parseRegionserverMetric(rsMetricsInfoJson,
                            "Hadoop:service=HBase,name=RegionServer,sub=Regions");
            Map<String, Object> tableLatenciesDataMap =
                    parseRegionserverMetric(rsMetricsInfoJson,
                            "Hadoop:service=HBase,name=RegionServer,sub=TableLatencies");
            Map<String, Object> tablesDataMap =
                    parseRegionserverMetric(rsMetricsInfoJson,
                            "Hadoop:service=HBase,name=RegionServer,sub=Tables");

            Set<String> tables = this.getTables(tablesDataMap);
            tablesMetrics  = new HashMap<>();
            for(String table : tables) {
                HBaseRsTableMetrics tableMetrics =
                        HBaseRsTableMetrics.parse(table, regionsDataMap, tableLatenciesDataMap);
                tablesMetrics.put(table, tableMetrics);
            }
        } catch (Exception e) {
            LOG.error("Get hbase regionserver metrics faield, node.host = " + this.getIp() + ", node.port = "+ this.getPort(), e);
        }
        this.status = this.jvmMetrics == null ? StatusEnum.OFFLINE : StatusEnum.ONLINE;
        return this;
    }

    /**
     * 获取当前 regionserver 有哪些表
     * @param tablesDataMap
     * @return
     */
    private Set<String> getTables(Map<String, Object> tablesDataMap) {
        Set<String> retTables = new HashSet<>();
        for(Map.Entry<String, Object> kv : tablesDataMap.entrySet()) {
            if(!kv.getKey().startsWith("Namespace_") ||
                    !kv.getKey().contains("_metric_")) continue;
            String line = kv.getKey().substring("Namespace_".length(), kv.getKey().lastIndexOf("_metric_"));
            String t =  line.replaceFirst("_table_", ":");
            if(retTables.contains(t)) continue;
            retTables.add(t);
        }
        return retTables;
    }

    public static Map parseRegionserverMetric(String json, String meticName) throws Exception{
        try {
            Map jsonMap = JSONObject.parseObject(json, HashMap.class);
            List<HashMap> metricList =
                    JSONObject.parseArray(JSONObject.toJSONString(jsonMap.get("beans")), HashMap.class);
            for(HashMap map : metricList) {
                if(!(map.get("name") + "").contains(meticName)) continue;
                return map;
            }
        }catch (Exception e) {
            throw new IllegalArgumentException("parse regionserver metric faield, metricName = "+ meticName, e);
        }
        throw new IllegalArgumentException("not found metric: "+ meticName);
    }

    private String getBaseRequestUrl() {
        return "http://" + this.getIp() + ":" + this.getPort() + "/jmx?qry=";
    }

    public StatusEnum getStatus() {
        return status;
    }

    public JvmMetrics getJvmMetrics() {
        return jvmMetrics;
    }

    public HBaseRsIpcMetrics getIpcMetrics() {
        return ipcMetrics;
    }

    public Map<String, HBaseRsTableMetrics> getTablesMetrics() {
        return tablesMetrics;
    }
}
