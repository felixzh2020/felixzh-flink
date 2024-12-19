import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author FelixZh
 * @desc 测试Flink Task Memory RES占用情况
 */
public class Kafka2ES {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job -d /path/to/jar /path/to/Kafka2Kafka.properties");
            System.exit(0);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("felixzh:9092")
                .setTopics("test")
                .setGroupId("felixzhGroup")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(50)))
                .process(new MyProcess<>())
                .addSink(MyEsSink.create("felixzh", 9200, "felixzh_index"));

        env.execute("Kafka2ES");
    }
}
