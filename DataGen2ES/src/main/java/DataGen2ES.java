import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author felixzh
 */
public class DataGen2ES {
    public static void main(String... args) throws Exception {
        if (args.length != 1) {
            System.out.println("Usage: flink run -t yarn-per-job /path/to/DataGen2ES-1.0.jar /path/to/DataGen2ES.properties");
            System.out.println("Usage: flink run-application -t yarn-application -Dclassloader.resolve-order=parent-first /path/to/DataGen2ES-1.0.jar /path/to/DataGen2ES.properties");
            System.exit(0);
        }

        ParameterTool param = ParameterTool.fromPropertiesFile(args[0]);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GeneratorFunction<Long, String> generatorFunction = index ->
                RandomPrintableDataGenerator.generate(param.getInt("source.record.size.byte", 512), true);

        DataGeneratorSource<String> source = new DataGeneratorSource<>(
                generatorFunction,
                Long.MAX_VALUE,
                //RateLimiterStrategy.noOp(),
                RateLimiterStrategy.perSecond(param.getInt("source.record.per.sec", 1)),
                Types.STRING);

        HttpHost[] esHosts = Arrays.stream(param.get("sink.es.list").split(","))
                .map(ipPort -> {
                    String[] ipPortList = ipPort.split(":");
                    return new HttpHost(ipPortList[0], Integer.parseInt(ipPortList[1]), "http");
                }).toArray(HttpHost[]::new);

        int bulkFlushMaxActions = param.getInt("sink.bulk.flush.max.actions", 1);
        long bulkFlushIntervalMs = param.getLong("sink.bulk.flush.interval.ms", 1000);
        int bulkFlushMaxMb = param.getInt("sink.bulk.flush.max.mb", 1);

        ElasticsearchSink<String> sink = new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(bulkFlushMaxActions)
                .setBulkFlushInterval(bulkFlushIntervalMs)
                .setBulkFlushMaxSizeMb(bulkFlushMaxMb)
                .setHosts(esHosts)
                .setEmitter((element, context, indexer) -> {
                            indexer.add(genIndexRequest(param.getRequired("sink.es.index"), element));
                        }
                )
                .setBulkFlushBackoffStrategy(FlushBackoffType.CONSTANT, 3, 500)
                .setConnectionTimeout(60000)
                .setConnectionRequestTimeout(60000)
                .setSocketTimeout(60000)
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "FelixZhDataGen")
                .setParallelism(param.getInt("source.parallelism", 2));

        stream.sinkTo(sink).setParallelism(param.getInt("sink.parallelism", 1));

        env.execute(DataGen2ES.class.getName() + " FelixZh");
    }

    private static IndexRequest genIndexRequest(String index, String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);
        //非必要不建议指定id，容易造成数据倾斜
        return Requests.indexRequest().index(index)/*.id(id)*/.source(json, XContentType.JSON);
    }
}
