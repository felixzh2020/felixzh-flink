import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Kafka2ES {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job -d /path/to/jar /path/to/Kafka2ES.properties");
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

        int flushMaxActions = parameterTool.getInt("sink.flush.actions", 1);
        int flushInterval = parameterTool.getInt("sink.flush.interval", 1);
        int flushSize = parameterTool.getInt("sink.flush.size", 1);

        HttpHost[] httpHosts = Arrays.stream(parameterTool.get("sink.es.ips").split(","))
                .map(node -> {
                    String[] ipPort = node.split(":");
                    return new HttpHost(ipPort[0], Integer.parseInt(ipPort[1]), "http");
                })
                .toArray(HttpHost[]::new);
        ElasticsearchSink<String> elasticsearchSink = new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(flushMaxActions)
                .setBulkFlushInterval(flushInterval)
                .setBulkFlushMaxSizeMb(flushSize)
                .setHosts(httpHosts)
                .setEmitter((element, context, indexer) ->
                        indexer.add(createIndexRequest(parameterTool.getRequired("sink.es.index"), element)))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 3, 500)
                .setConnectionTimeout(60000)
                .setConnectionRequestTimeout(60000)
                .setSocketTimeout(60000)
                .build();


        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .setParallelism(parameterTool.getInt("source.parallelism", 1));
        stream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect(s);
            }
        }).sinkTo(elasticsearchSink).setParallelism(parameterTool.getInt("sink.parallelism", 1));

        env.execute("Kafka2ES");
    }

    private static IndexRequest createIndexRequest(String index, String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);
        String id = String.valueOf(System.currentTimeMillis());
        return Requests.indexRequest().index(index).id(id).source(json);
    }
}
