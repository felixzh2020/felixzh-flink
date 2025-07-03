import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.hadoop.fs.Path;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class HBaseSinkFunctionV2<T> extends RichSinkFunction<T> implements CheckpointedFunction, BufferedMutator.ExceptionListener {
    private final Logger LOG = LoggerFactory.getLogger(HBaseSinkFunctionV2.class);

    private static final long serialVersionUID = 1L;

    private final ParameterTool parameterTool;
    private final String tableName;
    private final long writeBufferSize;
    private final long writeBufferPeriodicFlushTimeoutMs;
    private transient Connection connection;
    private transient BufferedMutator mutator;
    private final String hbaseConfPath;

    /**
     * This is set from inside the {@link BufferedMutator.ExceptionListener} if a {@link Throwable} was thrown.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public HBaseSinkFunctionV2(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
        tableName = parameterTool.getRequired("sink.table.name");
        writeBufferSize = parameterTool.getLong("sink.write.buffer.size", 2097152);
        writeBufferPeriodicFlushTimeoutMs = parameterTool.getLong("sink.write.buffer.period.flush.timeout.ms", 1000);
        hbaseConfPath = parameterTool.get("sink.hbase.conf.path", "");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // create a parameter instance, set the table name and custom listener reference.
        BufferedMutatorParams bufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(this);

        //Override the write buffer size specified by the provided Connection's org.apache.hadoop.conf.Configuration
        // instance, via the configuration key hbase.client.write.buffer.
        //即定量flush
        bufferedMutatorParams.writeBufferSize(writeBufferSize);

        //Set the max timeout before the buffer is automatically flushed.
        //即定时flush
        bufferedMutatorParams.setWriteBufferPeriodicFlushTimeoutMs(writeBufferPeriodicFlushTimeoutMs);

        org.apache.hadoop.conf.Configuration configuration = getHBaseClientConfiguration(parameterTool);
        if (!hbaseConfPath.isEmpty()) {
            configuration.addResource(new Path(hbaseConfPath));
        }
        connection = ConnectionFactory.createConnection(configuration);
        mutator = connection.getBufferedMutator(bufferedMutatorParams);

        //定时flush也可以通过该方法设置，等效
        //mutator.setWriteBufferPeriodicFlush(writeBufferPeriodicFlushTimeoutMs);
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value != null) {
            checkErrorAndRethrow();
            HBaseData hbaseData = (HBaseData) value;
            mutator.mutate(new Put(hbaseData.getRowKey()).addColumn(hbaseData.getColumnFamily(), hbaseData.getCfQualifier(), hbaseData.getData()));
        }
    }

    @Override
    public void close() throws Exception {
        if (mutator != null) {
            try {
                mutator.close();
            } catch (Exception e) {
                LOG.warn("Exception occurs while closing HBase BufferedMutator.", e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("Exception occurs while closing HBase Connection.", e);
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do.
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        flush();
    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
        // fail the sink and skip the rest of the items if the failure handler decides to throw an exception
        failureThrowable.compareAndSet(null, e);
    }

    private void flush() throws IOException {
        checkErrorAndRethrow();

        // BufferedMutator is thread-safe
        mutator.flush();
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
