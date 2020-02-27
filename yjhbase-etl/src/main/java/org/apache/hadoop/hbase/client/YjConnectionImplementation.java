package org.apache.hadoop.hbase.client;

import com.yjhbase.etl.jobs.imp.AbstractImpJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

/**
 * @author zhengzhubin
 * @date 2020/2/27
 * @description
 **/
public class YjConnectionImplementation extends ConnectionImplementation{
    /**
     * constructor
     *
     * @param conf Configuration object
     * @param pool
     * @param user
     */
    YjConnectionImplementation(Configuration conf, ExecutorService pool, User user) throws IOException {
        super(conf, pool, user);
    }

    @Override
    public ClientProtos.ClientService.BlockingInterface getClient(ServerName serverName) throws IOException {
        AbstractImpJob.jvmHost(); //设置虚拟 ip
        return super.getClient(serverName);
    }
}
