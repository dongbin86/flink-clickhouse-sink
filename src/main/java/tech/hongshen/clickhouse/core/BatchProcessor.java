package tech.hongshen.clickhouse.core;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Request;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.hongshen.clickhouse.common.ClickHouseConfig;
import tech.hongshen.clickhouse.common.FlushThreadFactory;

import java.io.Closeable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hongshen
 * @date 2020/12/24
 */
public class BatchProcessor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);

    private final AsyncHttpClient client;
    private volatile boolean closed = false;
    private final String targetTable;
    private final int maxFlushRows;
    private final int flushIntervalSec;
    private final Listener listener;
    private final ClickHouseConfig config;
    private TableRowsBuffer tableRowsBuffer;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture scheduledFuture;
    private final AtomicLong executionIdGen = new AtomicLong();


    public BatchProcessor(AsyncHttpClient client, ClickHouseConfig config, int taskIndex, String targetTable, int maxFlushRows, Listener listener) {
        this.client = client;
        this.targetTable = targetTable;
        this.maxFlushRows = maxFlushRows;
        this.config = config;
        this.flushIntervalSec = config.getFlushInterval();
        this.listener = listener;
        this.tableRowsBuffer = new TableRowsBuffer(targetTable);
        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
                1,
                new FlushThreadFactory((targetTable != null ? "[" + targetTable + "]" : "") + "timer-flusher", taskIndex)
        );
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new TimeFlusher(), flushIntervalSec, flushIntervalSec, TimeUnit.SECONDS);
    }

    public synchronized void add(String request) {
        ensureOpen();
        tableRowsBuffer.add(request);
        executeIfNeeded();
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (isOverTheLimit()) {
            execute();
        }
    }

    private boolean isOverTheLimit() {
        return maxFlushRows != -1 && tableRowsBuffer.bufferSize() >= maxFlushRows;
    }


    class TimeFlusher implements Runnable {
        TimeFlusher() {
        }

        public void run() {
            synchronized (BatchProcessor.this) {
                if (!closed) {
                    if (tableRowsBuffer.bufferSize() != 0) {
                        execute();
                    }
                }
            }
        }
    }


    private void execute() {
        final TableRowsBuffer tableRowsBuffer = this.tableRowsBuffer;
        final long executionId = executionIdGen.incrementAndGet();

        this.tableRowsBuffer = new TableRowsBuffer(targetTable);
        Request request = buildRequest(tableRowsBuffer);
        logger.info("Ready to load data to {}, size = {}", tableRowsBuffer.getTable(), tableRowsBuffer.getRows().size());

        boolean afterCalled = false;
        try {
            Response response = client.executeRequest(request).get();
            afterCalled = true;
            listener.handleResponse(executionId, tableRowsBuffer, response);
        } catch (Exception e) {
            if (!afterCalled) {
                listener.handleExceptionWhenGettingResponse(executionId, tableRowsBuffer, e);
            }
        }
    }


    private Request buildRequest(TableRowsBuffer tableRowsBuffer) {
        String csv = String.join(" , ", tableRowsBuffer.getRows());
        String query = String.format("INSERT INTO %s VALUES %s", tableRowsBuffer.getTable(), csv);
        String host = config.getConnectConfig().getRandomHostUrl();

        BoundRequestBuilder builder = client
                .preparePost(host)
                .setHeader(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8")
                .setBody(query);
        builder.setHeader(HttpHeaderNames.AUTHORIZATION, "Basic " + config.getConnectConfig().getCredentials());
        return builder.build();
    }

    public synchronized void flush() {
        ensureOpen();
        if (tableRowsBuffer.bufferSize() > 0) {
            execute();
        }
    }

    @Override
    public void close() {
        if (isOpen()) {
            closed = true;
            if (this.scheduledFuture != null) {
                cancel(this.scheduledFuture);
                this.scheduler.shutdown();
            }
            if (tableRowsBuffer.bufferSize() > 0) {
                execute();
            }
        }
    }


    public static boolean cancel(Future<?> toCancel) {
        if (toCancel != null) {
            return toCancel.cancel(false);
        }
        return false;
    }

    boolean isOpen() {
        return !this.closed;
    }

    protected void ensureOpen() {
        if (this.closed) {
            throw new IllegalStateException("batch process already closed");
        }
    }


    public interface Listener {
        void handleResponse(long executionId, TableRowsBuffer tableRowsBuffer, Response response);

        void handleExceptionWhenGettingResponse(long executionId, TableRowsBuffer tableRowsBuffer, Throwable failure);
    }
}
