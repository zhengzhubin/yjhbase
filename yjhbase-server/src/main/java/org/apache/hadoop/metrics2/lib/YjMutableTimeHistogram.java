package org.apache.hadoop.metrics2.lib;

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.CounterImpl;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhengzhubin
 * @date 2020/3/31
 * @description
 **/
public class YjMutableTimeHistogram extends MutableTimeHistogram{

    public YjMutableTimeHistogram(MetricsInfo info) {
        this(info.name(), info.description());
    }

    public YjMutableTimeHistogram(String name, String description) {
        super(name, description);
        this.prepare();
    }

    private List<Pair<String, CounterImpl>> yjRangeCounter = new ArrayList<>();
    private void prepare() {
        long priorRange = 0L;
        for(long r : this.getRanges()) {
            String tagName = name + "_yjTimeRangeCount_" + priorRange + "_" + r;
            yjRangeCounter.add(new Pair<>(tagName , new CounterImpl()));
            priorRange = r;
        }
        String tagName = name + "_yjTimeRangeCount_" + priorRange + "_" + Long.MAX_VALUE;
        yjRangeCounter.add(new Pair<>(tagName , new CounterImpl()));
    }

    @Override
    public void add(long val) {
        super.add(val);
        int at = 0;
        long[] ranges = this.getRanges();
        for(long r : ranges) {
            if(val < r) break;
            at ++;
        }
        yjRangeCounter.get(at).getSecond().increment();
    }

    @Override
    public void updateSnapshotRangeMetrics(MetricsRecordBuilder metricsRecordBuilder, Snapshot snapshot) {
        super.updateSnapshotRangeMetrics(metricsRecordBuilder, snapshot);
        for(Pair<String, CounterImpl> kv: this.yjRangeCounter) {
            if(kv.getSecond().getCount() <= 0) continue;
            metricsRecordBuilder.addCounter(Interns.info(kv.getFirst(), this.desc), kv.getSecond().getCount());
        }
    }
}
