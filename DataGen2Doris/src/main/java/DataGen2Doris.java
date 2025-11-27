import com.alibaba.fastjson.JSONObject;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;
import java.util.Random;

public class DataGen2Doris {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job /path/to/DataGen2Doris-1.0.jar /path/to/DataGen2Doris.properties");
            System.out.println("Usage: flink run-application -t yarn-application  -Dyarn.ship-files=DataGen2Doris.properties -Dclassloader.resolve-order=parent-first /path/to/DataGen2Doris-1.0.jar /path/to/DataGen2Doris.properties");
            System.out.println("Usage: flink run-application -t yarn-application  -Dyarn.ship-files=DataGen2Doris.properties -Dyarn.classpath.include-user-jar=DISABLED /path/to/DataGen2Doris-1.0.jar /path/to/DataGen2Doris.properties");
            System.exit(0);
        }

        ParameterTool param = ParameterTool.fromPropertiesFile(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        long sourceRecordSum = param.getLong("source.record.sum", Long.MAX_VALUE);
        int sourceRecordPerSec = param.getInt("source.record.per.sec", 1);

        GeneratorFunction<Long, String> generatorFunction = index -> buildData();

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                sourceRecordSum,
                //RateLimiterStrategy.noOp(),
                RateLimiterStrategy.perSecond(sourceRecordPerSec),
                Types.STRING);

        String sinkFeNodes = param.get("sink.fe.nodes");
        String sinkTable = param.get("sink.table");
        String sinkUserName = param.get("sink.username", "root");
        String sinkPassword = param.get("sink.password", "");

        DorisOptions dorisOptions = DorisOptions.builder()
                .setAutoRedirect(true)
                .setFenodes(sinkFeNodes)
                .setUsername(sinkUserName)
                .setPassword(sinkPassword)
                .setTableIdentifier(sinkTable)
                .build();

        int sinkBufferSize = param.getInt("sink.buffer.size", 1048576);
        int sinkBufferCount = param.getInt("sink.buffer.count", 3);
        int sinkBufferFlushIntervalMs = param.getInt("sink.buffer.flush.interval.ms", 10000);
        int sinkBufferFlushMaxBytes = param.getInt("sink.buffer.flush.max.bytes", 104857600);
        int sinkBufferFlushMaxRows = param.getInt("sink.buffer.flush.max.rows", 10000);
        Properties properties = new Properties();
        properties.setProperty("read_json_by_line", "true");
        properties.setProperty("format", "json");
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                .setLabelPrefix("label-doris")
                .setDeletable(false)
                .setBatchMode(true)
                .setStreamLoadProp(properties)
                .setBufferSize(sinkBufferSize)
                .setBufferCount(sinkBufferCount)
                .setBufferFlushIntervalMs(sinkBufferFlushIntervalMs)
                .setBufferFlushMaxBytes(sinkBufferFlushMaxBytes)
                .setBufferFlushMaxRows(sinkBufferFlushMaxRows)
                .build();

        DorisSink.Builder<String> builder = DorisSink.builder();
        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisOptions);

        int sourceParallelism = param.getInt("source.parallelism", 2);
        int sinkParallelism = param.getInt("sink.parallelism", 3);
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Doris Source By FelixZh")
                .setParallelism(sourceParallelism);
        dataStream.sinkTo(builder.build()).setParallelism(sinkParallelism);
        env.execute("FelixZh Demo");
    }

    private static String buildData() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", new Random().nextLong());
        jsonObject.put("data", System.currentTimeMillis());
        return jsonObject.toJSONString();
    }
}
