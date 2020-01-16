package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.util.FSHDFSUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/1/16
 * @description store files 合并策略调优
 **/
public class YjCompactionPolicy extends ExploringCompactionPolicy{

    private static final Log LOG = LogFactory.getLog(YjCompactionPolicy.class);

    /**
     * Constructor for ExploringCompactionPolicy.
     *
     * @param conf            The configuration object
     * @param storeConfigInfo An object to provide info about the store.
     */
    public YjCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
        super(conf, storeConfigInfo);
    }

    @Override
    public List<HStoreFile> applyCompactionPolicy(
            List<HStoreFile> candidates, boolean mightBeStuck, boolean mayUseOffPeak, int minFiles, int maxFiles) {

        List<HStoreFile> sfiles = this.sfilesSortByFileSize(candidates);
        final double currentRatio =
                mayUseOffPeak ? comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();
        long maxCompactSize = comConf.getMaxCompactSize(mayUseOffPeak);
        long minCompactSize = comConf.getMinCompactSize();

        List<HStoreFile> bestSelection = new ArrayList<>();
        if(mightBeStuck && sfiles.size() >= minFiles){ //在堵塞情况下,必须选举出一组文件进行合并
            bestSelection = sfiles.subList(0 , minFiles);
        }
        long bestSize = this.sfilesTotalSize(bestSelection);

        int end = 0;
        for(int start = 0; start < sfiles.size(); start ++){
            if(sfiles.get(start).getReader().length() > maxCompactSize) break;
            end ++;
        }
        if(end < minFiles) {
            if(bestSelection.size() > 0){
                String regionName = FSHDFSUtils.getPath(bestSelection.get(0).getPath().getParent());
                LOG.info("yjcompaction algorithm has selecte " + bestSelection.size() + "/" + candidates.size() + " files " +
                        "on region: " + regionName + " .(size = "+bestSize+ ", blocked ? " + mightBeStuck + ")");
            }
            return bestSelection;
        }

        //filter sfiles'size > maxCompactSize
        sfiles = sfiles.subList(0 , end);

        final double REPLACE_IF_BETTER_BY = 1.05;
        for(int i = 0 ;i< end ;i++){
            List<HStoreFile> betterSelection = new ArrayList<>();
            betterSelection.add(sfiles.get(i));
            long betterSize = sfiles.get(i).getReader().length();
            int numFiles = 1 , start = i + 1;
            while(start < end && numFiles < maxFiles){
                long currentFileSize = sfiles.get(start).getReader().length();
                if(currentFileSize <= minCompactSize) {
                    betterSelection.add(sfiles.get(start));
                    betterSize += currentFileSize;
                }
                else{
                    //if sum(filesize[i to start-1]) < filesize[start], break.
                    if(betterSize * Math.max(currentRatio , numFiles < 2 ? 2.0d : currentRatio) < currentFileSize) {
                        break;
                    }else{
                        betterSelection.add(sfiles.get(start));
                        betterSize += currentFileSize;
                    }
                }
                start ++; numFiles ++;
            }
            if(numFiles < minFiles) continue;
            if(bestSelection.size() == 0){
                bestSelection = betterSelection;
                bestSize = betterSize;
                continue;
            }
            double bestQuality = bestSize  / ((double) bestSelection.size() * REPLACE_IF_BETTER_BY) ;
            double betterQuality = betterSize / ((double) betterSelection.size());
            //sum(selected sfiles's size) / count(selected sfiles), more small more better
            if(bestQuality > betterQuality){
                bestSelection = betterSelection;
                bestSize = betterSize;
            }
        }

        if(bestSelection.size() > 0){
            String regionName = FSHDFSUtils.getPath(bestSelection.get(0).getPath().getParent());
            LOG.info("yjcompaction algorithm has selecte " + bestSelection.size() + "/" + candidates.size() + " files " +
                    "on region: " + regionName + " .(size = "+bestSize+ ", blocked ? " + mightBeStuck + ")");
        }
        return bestSelection;
    }

    /**
     * sum(storefiles.size)
     * @param sfiles
     * @return
     */
    private long sfilesTotalSize(List<HStoreFile> sfiles){
        long totalSize = 0;
        for(HStoreFile sfile : sfiles)
            totalSize += sfile.getReader().length();
        return totalSize;
    }

    /**
     * sort store files
     * @param storeFiles
     * @return
     */
    private List<HStoreFile> sfilesSortByFileSize(List<HStoreFile> storeFiles){
        HStoreFile[] sfiles = storeFiles.toArray(new HStoreFile[storeFiles.size()]);
        Arrays.sort(sfiles, new Comparator<HStoreFile>() {
            @Override
            public int compare(HStoreFile o1, HStoreFile o2) {
                return Long.compare(o1.getReader().length(), o2.getReader().length());
            }
        });
        return Arrays.asList(sfiles);
    }
}
