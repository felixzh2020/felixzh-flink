import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Kafka2HBase {

    private static final Logger LOG = LoggerFactory.getLogger(Kafka2HBase.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job -d /path/to/jar /path/to/Kafka2HBase.properties");
            System.exit(0);
        }

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(args[0]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long checkpointMS = parameterTool.getLong("env.checkpointing.timeMS", 0);
        if (checkpointMS != 0) {
            env.enableCheckpointing(checkpointMS, CheckpointingMode.EXACTLY_ONCE);
        }

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(parameterTool.get("source.brokers"))
                .setTopics(parameterTool.get("source.topic"))
                .setGroupId(parameterTool.get("source.groupId"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


        HBaseSinkFunction<HBaseData> hbaseSinkFunction = new HBaseSinkFunction<>(parameterTool);
        DataStream<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .setParallelism(parameterTool.getInt("source.parallelism", 1));
        DataStream<HBaseData> mapDataStream = sourceStream.map((MapFunction<String, HBaseData>) value -> {
            // 测试用例使用，正常需要从数据携带信息获取或者配置文件获取
            HBaseData hbaseData = new HBaseData();
            hbaseData.setRowKey(Bytes.toBytes(UUID.randomUUID().toString()));
            hbaseData.setColumnFamily(Bytes.toBytes("cf1"));
            hbaseData.setCfQualifier(Bytes.toBytes(UUID.randomUUID().toString()));
            hbaseData.setData(Bytes.toBytes(value));
            return hbaseData;
        });
        mapDataStream.addSink(hbaseSinkFunction).setParallelism(parameterTool.getInt("sink.parallelism", 1));
        env.execute("Kafka2HBase");
    }
}
