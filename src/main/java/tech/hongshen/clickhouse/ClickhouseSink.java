package tech.hongshen.clickhouse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.hongshen.clickhouse.common.ClickHouseConfig;
import tech.hongshen.clickhouse.core.BatchProcessor;
import tech.hongshen.clickhouse.core.clickhouseRowCollector;
import tech.hongshen.clickhouse.core.TableRowsBuffer;
import tech.hongshen.clickhouse.exception.ClickhouseException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static tech.hongshen.clickhouse.common.ClickhouseConstants.BATCH_SIZE;
import static tech.hongshen.clickhouse.common.ClickhouseConstants.TARGET_TABLE_NAME;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class ClickhouseSink extends RichSinkFunction<String> implements CheckpointedFunction {

    private static final long serialVersionUID = 8882341085011937977L;

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    private final Properties props;

    private final ClickHouseConfig config;


    private AtomicLong numPendingRows = new AtomicLong(0);
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    private transient AsyncHttpClient client;
    private transient BatchProcessor batchProcessor;
    private transient clickhouseRowCollector clickhouseRowCollector;
    private boolean flushOnCheckpoint = true;

    public ClickhouseSink(Properties props) {
        this.props = props;
        this.config = new ClickHouseConfig(props);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = Dsl.asyncHttpClient();
        batchProcessor = new BatchProcessor(
                client,
                config,
                getRuntimeContext().getIndexOfThisSubtask(),
                props.getProperty(TARGET_TABLE_NAME, ""),
                Integer.parseInt(props.getProperty(BATCH_SIZE, "10000")),
                new BatchProcessorListener()
        );
        clickhouseRowCollector = new clickhouseRowCollector(batchProcessor, flushOnCheckpoint, numPendingRows);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        checkErrorAndRethrow();
        clickhouseRowCollector.collect(value);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkErrorAndRethrow();
        if (flushOnCheckpoint) {
            while (numPendingRows.get() != 0) {
                batchProcessor.flush();
                checkErrorAndRethrow();
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // no initialization needed
    }

    @Override
    public void close() throws Exception {
        if (batchProcessor != null) {
            batchProcessor.close();
            batchProcessor = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }
        checkErrorAndRethrow();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in ClickhouseSink.", cause);
        }
    }


    private class BatchProcessorListener implements BatchProcessor.Listener {

        private static final int HTTP_OK = 200;

        @Override
        public void handleResponse(long executionId, TableRowsBuffer tableRowsBuffer, Response response) {
            if (response.getStatusCode() != HTTP_OK) {
                String errorString = response.getResponseBody();
                logger.error("Failed to send data to ClickHouse,  ClickHouse response = {}. ", errorString);
                failureThrowable.compareAndSet(null, new ClickhouseException(errorString));
            } else {
                logger.info("Successful send data to ClickHouse, batch size = {}, target table = {}", tableRowsBuffer.bufferSize(), tableRowsBuffer.getTable());
            }
            if (flushOnCheckpoint) {
                numPendingRows.getAndAdd(-tableRowsBuffer.bufferSize());
            }
        }

        @Override
        public void handleExceptionWhenGettingResponse(long executionId, TableRowsBuffer tableRowsBuffer, Throwable failure) {
            logger.error("Failed to send data to ClickHouse: {}", failure.getMessage(), failure.getCause());
            failureThrowable.compareAndSet(null, new ClickhouseException(failure.getMessage(), failure.getCause()));
            if (flushOnCheckpoint) {
                numPendingRows.getAndAdd(-tableRowsBuffer.bufferSize());
            }
        }
    }


}
