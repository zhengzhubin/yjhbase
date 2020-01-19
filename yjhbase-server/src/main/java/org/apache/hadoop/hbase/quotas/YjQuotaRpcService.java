package org.apache.hadoop.hbase.quotas;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.regionserver.Region;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zhengzhubin
 * @date 2020/1/17
 * @description 统一管理需要回流的任务
 **/
public class YjQuotaRpcService {

    protected static final Log LOG = LogFactory.getLog(YjQuotaRpcService.class);

    //请求触发限流时是否将请求重新塞回队列
    public static final String HBASE_QUOTA_REQUESTS_REQUEUE_ONFAIL = "hbase.quota.requests.requeue.onfail";
    //请求堵塞时间最长
    public static final String HBASE_QUOTA_REQUEST_PAUSE_MAX_MS = "hbase.quota.request.pause.max.ms";
    public static long pauseLimitInMimllis = 5000L;
    public static boolean requeueOnFail = true;

    static{
        Configuration conf = HBaseConfiguration.create();
        pauseLimitInMimllis = conf.getLong(HBASE_QUOTA_REQUEST_PAUSE_MAX_MS , pauseLimitInMimllis);
        requeueOnFail = conf.getBoolean(HBASE_QUOTA_REQUESTS_REQUEUE_ONFAIL , true);
        CallRequeueThread callRequeueThread = new CallRequeueThread();
        callRequeueThread.setName("RpcCallRequeueThread");
        callRequeueThread.start();
    }

    private static List<CallRunner> calls = new ArrayList<>();

    /**
     * 等待重新排队
     * @param call
     */
    public static void waitForRequeue(CallRunner call){
        synchronized (calls){
            calls.add(call);
        }
    }

    /**
     * 获取任务
     * @param call
     * @return
     */
    public static long getCallInQueueMillis(RpcCall call){
        long waitTS = System.currentTimeMillis() - call.getReceiveTime();
        return waitTS > 0 ? waitTS : 0L;
    }


    /**
     * 请求堵塞可以在队列的最长时间
     * @return
     */
    public static long getPauseLimitInMimllis(){
        return pauseLimitInMimllis;
    }

    /**
     * 不受限quota
     * @return
     */
    public static OperationQuota getUnlimitedQuota(){
        return NoopOperationQuota.get();
    }

    static Map<String , Map<String , Long>> tablesRequests = new HashMap<>();
    static long latestLOG = System.currentTimeMillis();
    /**
     * 记录遇到限流的请求
     * @param r
     */
    public static synchronized void logRequestsOnQuotaLimit(Region r, String reqType){
        long currentTS = System.currentTimeMillis();
        if(currentTS - 60000L > latestLOG){
            for(Map.Entry<String, Map<String, Long>> tKV : tablesRequests.entrySet()){
                for(Map.Entry<String, Long> kv : tKV.getValue().entrySet()){
                    LOG.warn("[tablesOnQuotaLimit, " + (currentTS - latestLOG)/1000 + "] table = " + tKV.getKey() + " : {" + kv.getKey() + " : " + kv.getValue() + "}");
                }
            }
            tablesRequests = new HashMap<>();
            latestLOG = currentTS;
        }
        String table = r.getTableDescriptor().getTableName().getNameAsString();
        if(!tablesRequests.containsKey(table))
            tablesRequests.put(table , new HashMap<String , Long>());
        Map<String, Long> tmap = tablesRequests.get(table);
        if(!tmap.containsKey(reqType)) {
            tmap.put(reqType , 1L);
        }else{
            tmap.put(reqType , tmap.get(reqType) + 1);
        }
    }

    /**
     * 定时将触发限流的请求重新塞回任务队列 ，避免任务不停被调度
     */
    static class CallRequeueThread extends Thread{
        @Override
        public void run(){
            while(true){
                try {
                    Thread.sleep(300L);
                    this.requeue();
                } catch (Exception e) {}
            }
        }

        private void requeue() throws Exception {
            List<CallRunner> tasks_ = new ArrayList<>();
            int numCalls = 0;
            synchronized (YjQuotaRpcService.calls){
                numCalls = YjQuotaRpcService.calls.size();
                for(int i = 0 ;i < numCalls ;i++){
                    tasks_.add(YjQuotaRpcService.calls.remove(0));
                }
            }
            if(numCalls == 0) return ;
            YjQuotaRpcService.LOG.warn("there's " + numCalls + " tasks need to be requeued.");
            for(CallRunner task : tasks_){
                task.requeue();
            }
        }
    }
}

