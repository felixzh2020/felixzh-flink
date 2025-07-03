import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class HBaseSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction, BufferedMutator.ExceptionListener {
    private final Logger LOG = LoggerFactory.getLogger(HBaseSinkFunction.class);

    private static final long serialVersionUID = 1L;
    private final String tableName;

    /**
     * 注意：增加 transient
     * The implementation of the FlinkUserCodeClassLoader is not serializable. The object probably contains or references non serializable fields.
     * org.apache.flink.api.java.ClosureCleaner.clean(ClosureCleaner.java:164)
     * org.apache.flink.api.java.ClosureCleaner.clean(ClosureCleaner.java:132)
     * org.apache.flink.api.java.ClosureCleaner.clean(ClosureCleaner.java:132)
     * org.apache.flink.api.java.ClosureCleaner.clean(ClosureCleaner.java:69)
     * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.clean(StreamExecutionEnvironment.java:2317)
     * org.apache.flink.streaming.api.datastream.DataStream.clean(DataStream.java:202)
     * org.apache.flink.streaming.api.datastream.DataStream.addSink(DataStream.java:1244)
     * Kafka2HBase.main(Kafka2HBase.java:57)
     */
    private final ParameterTool parameterTool;
    private final long bufferFlushMaxSizeInBytes;
    private final long bufferFlushMaxMutations;
    private final long bufferFlushIntervalMillis;

    private transient Connection connection;
    private transient BufferedMutator mutator;

    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;
    private transient AtomicLong numPendingRequests;

    private transient volatile boolean closed = false;

    /**
     * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable}
     * was thrown.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the
     * sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public HBaseSinkFunction(ParameterTool parameterTool) {
        this.tableName = parameterTool.getRequired("sink.table.name");
        this.parameterTool = parameterTool;
        this.bufferFlushIntervalMillis = parameterTool.getLong("sink.flush.interval", 1000);
        this.bufferFlushMaxMutations = parameterTool.getLong("sink.flush.mutation", 1000);
        this.bufferFlushMaxSizeInBytes = parameterTool.getLong("sink.flush.size", 1048576);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            this.numPendingRequests = new AtomicLong(0);

            if (null == connection) {
                this.connection = ConnectionFactory.createConnection(getHBaseClientConfiguration(parameterTool));
            }
            // create a parameter instance, set the table name and custom listener reference.
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(this);
            if (bufferFlushMaxSizeInBytes > 0) {
                // 达到数据总量flush
                params.writeBufferSize(bufferFlushMaxSizeInBytes);
            }
            this.mutator = connection.getBufferedMutator(params);

            // 定时flush
            if (bufferFlushIntervalMillis > 0 && bufferFlushMaxMutations != 1) {
                this.executor = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("hbase-upsert-sink-flusher"));
                this.scheduledFuture = this.executor.scheduleWithFixedDelay(() -> {
                    if (closed) {
                        return;
                    }
                    try {
                        flush();
                    } catch (Exception e) {
                        // fail the sink and skip the rest of the items
                        // if the failure handler decides to throw an exception
                        failureThrowable.compareAndSet(null, e);
                    }
                }, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS);
            }
        } catch (TableNotFoundException tnfe) {
            LOG.error("The table " + tableName + " not found ", tnfe);
            throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            LOG.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }

    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkErrorAndRethrow();
        HBaseData hbaseData = (HBaseData) value;
        mutator.mutate(new Put(hbaseData.getRowKey()).addColumn(hbaseData.getColumnFamily(), hbaseData.getCfQualifier(), hbaseData.getData()));

        // 达到数据行数flush
        if (bufferFlushMaxMutations > 0 && numPendingRequests.incrementAndGet() >= bufferFlushMaxMutations) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;

        if (mutator != null) {
            try {
                mutator.close();
            } catch (IOException e) {
                LOG.warn("Exception occurs while closing HBase BufferedMutator.", e);
            }
            this.mutator = null;
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                LOG.warn("Exception occurs while closing HBase Connection.", e);
            }
            this.connection = null;
        }

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        while (numPendingRequests.get() != 0) {
            flush();
        }
    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
        // fail the sink and skip the rest of the items
        // if the failure handler decides to throw an exception
        failureThrowable.compareAndSet(null, e);
    }

    private void flush() throws IOException {
        // BufferedMutator is thread-safe
        mutator.flush();
        numPendingRequests.set(0);
        checkErrorAndRethrow();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in HBaseSink.", cause);
        }
    }

    private org.apache.hadoop.conf.Configuration getHBaseClientConfiguration(ParameterTool parameterTool) {
        org.apache.hadoop.conf.Configuration hbaseClientConf = new org.apache.hadoop.conf.Configuration();
        // 借鉴Configuration result = HBaseConfiguration.create();
        hbaseClientConf.setClassLoader(HBaseConfiguration.create().getClassLoader());

        final Map<String, String> hbaseClientProperties = parameterTool.toMap();
        final String HBASE_CLIENT_PREFIX = "sink.properties.";

        if (hbaseClientProperties.keySet().stream().anyMatch(key -> key.startsWith(HBASE_CLIENT_PREFIX))) {
            hbaseClientProperties.keySet().stream().filter(key -> key.startsWith(HBASE_CLIENT_PREFIX)).forEach(key -> {
                final String subKey = key.substring(HBASE_CLIENT_PREFIX.length());
                final String value = hbaseClientProperties.get(key);
                hbaseClientConf.set(subKey, value);
            });
        }
        hbaseClientConf.forEach(entry -> LOG.info("====" + entry.toString()));
        return hbaseClientConf;
    }
}
