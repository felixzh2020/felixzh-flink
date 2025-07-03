import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.nio.charset.StandardCharsets;

public class CustomSource<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {

    private final long dataByteSize;
    private final long sleepTimeMs;
    private transient String data;
    private volatile boolean isRunning = true;

    public CustomSource(long dataByeSize, long sleepTimeMs) {
        this.dataByteSize = dataByeSize;
        this.sleepTimeMs = sleepTimeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        data = buildData(dataByteSize);
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (isRunning) {
            sourceContext.collect(data);
            Thread.sleep(sleepTimeMs);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // nothing to do
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // nothing to do
    }

    private String buildData(long targetBytes) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            sb.append("a");
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            if (bytes.length >= targetBytes) {
                return sb.toString();
            }
        }
    }
}
