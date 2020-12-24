package tech.hongshen.clickhouse.core;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class clickhouseRowCollector {

    private final BatchProcessor batchProcessor;
    private final boolean flushOnCheckpoint;
    private final AtomicLong numPendingRowsRef;

    public clickhouseRowCollector(BatchProcessor batchProcessor, boolean flushOnCheckpoint, AtomicLong numPendingRowsRef) {
        this.batchProcessor = checkNotNull(batchProcessor);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.numPendingRowsRef = checkNotNull(numPendingRowsRef);
    }

    public void collect(String... rows) {
        for (String row : rows) {
            if (flushOnCheckpoint) {
                numPendingRowsRef.getAndIncrement();
            }
            this.batchProcessor.add(row);
        }
    }


}
