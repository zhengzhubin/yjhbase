package com.yjhbase.manager.utils;

import com.yjhbase.manager.common.ServerConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;


/**
 * @author zhengzhubin
 * @date 2020/4/26
 * @description
 **/
@Component
public class HBaseClientUtil {

    private static Logger LOG = LoggerFactory.getLogger(HBaseClientUtil.class);

    @Autowired
    private ServerConfig serverConfig;

    private Connection connection = null;

    public synchronized Connection getConnection() throws IOException {
        if(connection != null && !connection.isClosed()) return connection;
        connection = ConnectionFactory.createConnection(this.getConnectionConf());
        return connection;
    }

    public Admin getAdmin() throws IOException {
        return this.getConnection().getAdmin();
    }

    public HTable getTable(String tablename) throws IOException {
        return (HTable) this.getConnection().getTable(TableName.valueOf(tablename));
    }

    public boolean tableExists(String tablename) throws IOException {
        return this.getAdmin().tableExists(TableName.valueOf(tablename));
    }

    public void createTable(String tablename, String cf, byte[][] splitKeys, Map<String, String> values) throws IOException {
        if(this.tableExists(tablename))
            throw new RuntimeException(String.format("table %s has already exists.", tablename));

        values.forEach((k, v) -> {

        });
        TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tablename));
//        tableDesc.setColumnFamily()

    }

    private Configuration getConnectionConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, this.serverConfig.getZkNodes());
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT , this.serverConfig.getZkPort());
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, this.serverConfig.getZkParent());
        conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 60000);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
        conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 100);
        conf.setInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS, 1000);
        conf.setInt(HConstants.HBASE_CLIENT_MAX_PERSERVER_TASKS, 100);
        conf.setInt(HConstants.HBASE_CLIENT_MAX_PERREGION_TASKS, 20);
        conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
        conf.setInt(HConstants.HBASE_RPC_READ_TIMEOUT_KEY, 60000);
        conf.setInt(HConstants.HBASE_RPC_WRITE_TIMEOUT_KEY, 60000);
        return conf;
    }
}
