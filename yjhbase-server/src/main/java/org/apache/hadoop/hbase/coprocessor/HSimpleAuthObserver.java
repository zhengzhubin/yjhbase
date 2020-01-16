package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.RpcServer;

import java.io.IOException;
import java.net.InetAddress;

/**
 * @author zhengzhubin
 * @date 2020/1/16
 * @description：hbase基础权限认证协处理器
 **/
public class HSimpleAuthObserver implements MasterObserver{

    private static final Log LOG = LogFactory.getLog(HSimpleAuthObserver.class);
    private static final String HBASE_AUTH_USER_ADMIN = "yjhbase.auth.user.admin";

    private static String localHost = null;
    static {
        try {
            localHost = InetAddress.getLocalHost().getHostAddress();
        } catch(Exception e) {
            LOG.error("load server local address failed.", e);
        }
    }

    /**
     * 验证客户端用户是否管理员用户
     * @param conf
     * @throws IOException
     */
    public void isValidUser(Configuration conf , String requestInfo) throws IOException{
        if(StringUtils.isBlank(localHost)) return;
        RpcServer.getRemoteAddress().get();
        InetAddress remoteAddress = RpcServer.getRemoteAddress().get();
        if(remoteAddress == null) return;
        String remoteHost = remoteAddress.getHostAddress();
        if(StringUtils.isBlank(remoteHost) || localHost.equals(remoteHost)) return;

        String adminUser = conf.get(HBASE_AUTH_USER_ADMIN);
        if(StringUtils.isBlank(adminUser)) return;
        String remoteUser = RpcServer.getRequestUserName().get();
        LOG.info("remote client("+remoteHost+"/"+remoteUser+") for request: " + requestInfo);
        if(remoteUser == null || !remoteUser.equals(adminUser))
            throw new IOException("invalid remote client("+remoteHost+"/"+remoteUser+") for request: " + requestInfo);
    }

    @Override
    public  void preCreateTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions)
            throws IOException {
        if(desc.isMetaTable()) return;
        String requestInfo = "create htable: "+ desc.getTableName().getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preDeleteTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
        String requestInfo = "drop htable: "+ tableName.getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preTruncateTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tableName)
            throws IOException {
        String requestInfo = "truncate htable: "+ tableName.getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preModifyTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName, TableDescriptor newDescriptor)
            throws IOException {
        String requestInfo = "modify htable: "+ tableName.getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preEnableTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
            throws IOException {
        String requestInfo = "enable htable: "+ tableName.getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preDisableTable(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, final TableName tableName)
            throws IOException {
        String requestInfo = "disable htable: "+ tableName.getNameAsString();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preCreateNamespace(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor ns)
            throws IOException {
        String requestInfo = "create namespace: "+ ns.getName();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preDeleteNamespace(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, String namespace)
            throws IOException {
        String requestInfo = "drop namespace: "+ namespace;
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

    @Override
    public void preModifyNamespace(
            final ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor newNsDescriptor)
            throws IOException {
        String requestInfo = "modify namespace: "+ newNsDescriptor.getName();
        this.isValidUser(ctx.getEnvironment().getConfiguration() , requestInfo);
    }

}

